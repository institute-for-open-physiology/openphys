package com.vineyardfinance.sensors.update

import com.google.gson.Gson
import com.google.gson.JsonObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

/**
 * Client for the QR-as-update flow: a scanned enrollment link, previewed
 * without consuming its one-use exchange, authorizes checking the served APK
 * version and downloading the artifact through the short-lived ticket the
 * server mints. The token travels only in the POST body; the download is
 * hash-verified against the server-declared digest before it is offered to
 * the package installer.
 */
class AppUpdateClient(
    private val baseUrl: String,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .build(),
) {
    data class ApkPreview(
        val versionCode: Long,
        val versionName: String,
        val sha256: String,
        val expiresAtUnixMs: Long,
        val ticket: String?,
    )

    init {
        require(
            baseUrl.startsWith("https://") ||
                baseUrl.startsWith("http://127.0.0.1") ||
                baseUrl.startsWith("http://localhost"),
        ) { "server URL must use HTTPS" }
    }

    suspend fun preview(enrollmentToken: String): ApkPreview = withContext(Dispatchers.IO) {
        val body = JsonObject().apply { addProperty("enrollment_token", enrollmentToken) }
        val request = Request.Builder()
            .url(baseUrl.trimEnd('/') + "/api/v1/device-enrollments/preview")
            .post(body.toString().toRequestBody("application/json".toMediaType()))
            .header("Accept", "application/json")
            .header("Cache-Control", "no-store")
            .header("Origin", baseUrl.trimEnd('/'))
            .build()
        client.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            if (!response.isSuccessful) {
                throw IOException("update check failed (HTTP ${response.code})")
            }
            val json = Gson().fromJson(text, JsonObject::class.java)
                ?: throw IOException("empty update-check response")
            val apk = json.getAsJsonObject("apk")
                ?: throw IOException("update-check response lacks APK metadata")
            val ticket = response.headers("Set-Cookie")
                .firstOrNull { it.startsWith("sensors_apk_ticket=") }
                ?.substringAfter("sensors_apk_ticket=")
                ?.substringBefore(';')
                ?.takeIf { it.isNotBlank() }
            ApkPreview(
                versionCode = apk.get("version_code")?.asLong
                    ?: throw IOException("update-check response lacks a version code"),
                versionName = apk.get("version_name")?.asString.orEmpty(),
                sha256 = apk.get("sha256")?.asString.orEmpty(),
                expiresAtUnixMs = json.get("expires_at_unix_ms")?.asLong ?: 0L,
                ticket = ticket,
            )
        }
    }

    /** Streams the APK to [destination] and fails closed on digest mismatch. */
    suspend fun downloadVerified(
        ticket: String,
        expectedSha256: String,
        destination: File,
    ): File = withContext(Dispatchers.IO) {
        require(expectedSha256.matches(Regex("^[0-9a-f]{64}$"))) { "invalid expected APK digest" }
        val request = Request.Builder()
            .url(baseUrl.trimEnd('/') + "/downloads/training-sensors.apk")
            .header("Cookie", "sensors_apk_ticket=$ticket")
            .header("Cache-Control", "no-store")
            .build()
        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw IOException("update download failed (HTTP ${response.code})")
            }
            destination.parentFile?.mkdirs()
            val temporary = File(destination.parentFile, destination.name + ".part")
            val digest = MessageDigest.getInstance("SHA-256")
            temporary.outputStream().use { output ->
                val source = response.body?.byteStream()
                    ?: throw IOException("empty update download")
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val read = source.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                    output.write(buffer, 0, read)
                }
                output.fd.sync()
            }
            val actual = digest.digest().joinToString("") { "%02x".format(it) }
            if (actual != expectedSha256) {
                temporary.delete()
                throw IOException("update download failed integrity verification")
            }
            if (!temporary.renameTo(destination)) {
                temporary.delete()
                throw IOException("could not finalize the update download")
            }
            destination
        }
    }
}
