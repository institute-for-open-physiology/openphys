package com.vineyardfinance.sensors.auth

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.network.HttpJson
import java.nio.charset.StandardCharsets
import java.util.UUID

data class EnrollmentMetadata(
    val displayLabel: String?,
    val androidManufacturer: String,
    val androidModel: String,
    val androidSdk: Int,
    val appPackage: String,
    val appVersionName: String,
    val appVersionCode: Long,
)

data class EnrollmentResult(val installationId: String, val credentialId: String)

interface EnrollmentExchange {
    suspend fun exchange(enrollmentToken: String, exchangeId: String): EnrollmentResult
}

class EnrollmentClient(
    private val http: HttpJson,
    private val identity: AppIdentity,
    private val credentials: DeviceCredentialStore,
    private val metadata: () -> EnrollmentMetadata,
) : EnrollmentExchange {
    private val gson = Gson()

    override suspend fun exchange(enrollmentToken: String, exchangeId: String): EnrollmentResult {
        require(TOKEN.matches(enrollmentToken)) { "invalid enrollment token" }
        val parsedExchange = UUID.fromString(exchangeId).toString()
        val details = metadata()
        require(details.appVersionCode >= 0) { "app version code must not be negative" }
        val request = JsonObject().apply {
            addProperty("enrollment_token", enrollmentToken)
            addProperty("installation_id", identity.installationId)
            addProperty("exchange_id", parsedExchange)
            if (details.displayLabel == null) {
                add("display_label", null)
            } else {
                addProperty("display_label", details.displayLabel.boundedUtf8(DISPLAY_LABEL_MAX_BYTES))
            }
            addProperty(
                "android_manufacturer",
                details.androidManufacturer.boundedUtf8(ANDROID_MANUFACTURER_MAX_BYTES),
            )
            addProperty("android_model", details.androidModel.boundedUtf8(ANDROID_MODEL_MAX_BYTES))
            addProperty("android_sdk", details.androidSdk)
            addProperty("app_package", details.appPackage.boundedUtf8(APP_PACKAGE_MAX_BYTES))
            addProperty("app_version_name", details.appVersionName.boundedUtf8(APP_VERSION_NAME_MAX_BYTES))
            addProperty("app_version_code", details.appVersionCode)
        }
        val response = http.post(EXCHANGE_PATH, gson.toJson(request)).requireSuccess()
        val json = http.jsonObject(response.body)
        val installationId = HttpJson.requiredUuid(json, "installation_id")
        require(installationId == identity.installationId) { "enrollment installation mismatch" }
        val credentialId = HttpJson.requiredUuid(json, "credential_id")
        val credential = HttpJson.requiredString(json, "device_credential")
        require(TOKEN.matches(credential)) { "invalid device credential response" }
        credentials.storeCredential(credential)
        return EnrollmentResult(installationId, credentialId)
    }

    private fun String.boundedUtf8(maxBytes: Int): String {
        val value = trim()
        if (value.toByteArray(StandardCharsets.UTF_8).size <= maxBytes) return value
        val bounded = StringBuilder(value.length)
        var index = 0
        var usedBytes = 0
        while (index < value.length) {
            val codePoint = value.codePointAt(index)
            val charCount = Character.charCount(codePoint)
            val nextIndex = index + charCount
            val chunk = value.substring(index, nextIndex)
            val chunkBytes = chunk.toByteArray(StandardCharsets.UTF_8).size
            if (usedBytes + chunkBytes > maxBytes) break
            bounded.append(chunk)
            usedBytes += chunkBytes
            index = nextIndex
        }
        return bounded.toString()
    }

    companion object {
        const val EXCHANGE_PATH = "/api/v1/device-enrollments/exchange"
        private val TOKEN = Regex("^[A-Za-z0-9_-]{43}$")
        private const val DISPLAY_LABEL_MAX_BYTES = 80
        private const val ANDROID_MANUFACTURER_MAX_BYTES = 80
        private const val ANDROID_MODEL_MAX_BYTES = 80
        private const val APP_PACKAGE_MAX_BYTES = 200
        private const val APP_VERSION_NAME_MAX_BYTES = 80
    }
}
