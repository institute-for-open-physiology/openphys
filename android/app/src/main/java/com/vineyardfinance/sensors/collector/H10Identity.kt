package com.vineyardfinance.sensors.collector

object H10Identity {
    private val serialPattern = Regex("^[A-Z0-9]{4,32}$")

    fun configuredSerial(value: String): String {
        val serial = value.trim().uppercase().removePrefix("POLAR H10 ").trim()
        require(serialPattern.matches(serial)) {
            "H10 target must be the 4-32 character serial shown after 'Polar H10'"
        }
        return serial
    }

    fun advertisedName(configuredTarget: String): String =
        "Polar H10 ${configuredSerial(configuredTarget)}"

    fun physicalDeviceId(configuredTarget: String): String =
        "h10_${configuredSerial(configuredTarget).lowercase()}"

    fun verifyObservedSerial(configuredTarget: String, observedSerial: String?): VerifiedH10Identity {
        val configured = configuredSerial(configuredTarget)
        val observed = observedSerial?.trim()?.uppercase().orEmpty()
        require(serialPattern.matches(observed)) { "H10 Device Information serial is missing or invalid" }
        require(observed == configured) {
            "H10 serial mismatch: configured=$configured observed=$observed"
        }
        return VerifiedH10Identity(configured, observed, physicalDeviceId(configured))
    }
}

data class VerifiedH10Identity(
    val configuredSerial: String,
    val observedSerial: String,
    val physicalDeviceId: String,
)
