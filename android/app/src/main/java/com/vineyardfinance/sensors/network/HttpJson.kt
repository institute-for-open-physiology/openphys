package com.vineyardfinance.sensors.network

import com.google.gson.Gson
import com.google.gson.JsonObject
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class HttpJson(
    val baseUrl: String,
    private val client: OkHttpClient = defaultClient(),
) {
    private val gson = Gson()

    init {
        require(baseUrl.startsWith("https://") || baseUrl.startsWith("http://127.0.0.1") || baseUrl.startsWith("http://localhost")) {
            "server URL must use HTTPS"
        }
    }

    suspend fun post(path: String, bodyJson: String, authorization: String? = null): HttpResult {
        val request = Request.Builder()
            .url(baseUrl.trimEnd('/') + path)
            .post(bodyJson.toRequestBody(JSON))
            .header("Accept", "application/json")
            .header("Cache-Control", "no-store")
            .apply { if (authorization != null) header("Authorization", authorization) }
            .build()
        // enqueue + invokeOnCancellation(Call.cancel) instead of a blocking execute():
        // coroutine cancellation must actually tear the socket down, so a request stuck in
        // a state the timeouts fail to bound cannot outlive its caller (upload watchdog).
        val call = client.newCall(request)
        return suspendCancellableCoroutine { continuation ->
            continuation.invokeOnCancellation { call.cancel() }
            call.enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    if (continuation.isActive) continuation.resumeWithException(e)
                }

                override fun onResponse(call: Call, response: Response) {
                    val result = runCatching {
                        response.use {
                            HttpResult(it.code, it.body?.string().orEmpty(), it.header("X-Request-Id"))
                        }
                    }
                    if (continuation.isActive) {
                        result.fold(continuation::resume) { error ->
                            continuation.resumeWithException(
                                error as? IOException ?: IOException("response read failed", error),
                            )
                        }
                    }
                }
            })
        }
    }

    fun jsonObject(text: String): JsonObject = try {
        gson.fromJson(text, JsonObject::class.java) ?: throw IOException("empty JSON object")
    } catch (error: RuntimeException) {
        throw IOException("invalid JSON response", error)
    }

    data class HttpResult(val code: Int, val body: String, val requestId: String?) {
        val successful: Boolean get() = code in 200..299
        fun requireSuccess(): HttpResult {
            if (!successful) throw HttpStatusException(code, requestId, responseMachineCode(body))
            return this
        }
    }

    class HttpStatusException(
        val status: Int,
        requestId: String?,
        val machineCode: String? = null,
    ) :
        IOException("HTTP $status${requestId?.let { " request_id=$it" }.orEmpty()}")

    companion object {
        private val JSON = "application/json; charset=utf-8".toMediaType()

        fun requiredString(json: JsonObject, name: String): String =
            json.get(name)?.takeUnless { it.isJsonNull }?.asString?.takeIf { it.isNotBlank() }
                ?: error("missing response field $name")

        fun requiredLong(json: JsonObject, name: String): Long =
            json.get(name)?.takeUnless { it.isJsonNull }?.asLong
                ?: error("missing response field $name")

        fun requiredUuid(json: JsonObject, name: String): String =
            UUID.fromString(requiredString(json, name)).toString()

        private fun responseMachineCode(body: String): String? = runCatching {
            com.google.gson.JsonParser.parseString(body).asJsonObject
                .get("code")
                ?.takeUnless { it.isJsonNull }
                ?.asString
                ?.takeIf { it.isNotBlank() }
        }.getOrNull()

        fun defaultClient(): OkHttpClient = OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .callTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()
    }
}
