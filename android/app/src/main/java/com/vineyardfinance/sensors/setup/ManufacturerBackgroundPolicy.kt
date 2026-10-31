package com.vineyardfinance.sensors.setup

/**
 * Per-manufacturer background-execution guidance for the guided setup.
 *
 * Testers can run any Android phone, not just the Samsung used in early
 * field tests. Where a reliable settings deep link exists (currently only
 * Samsung Device care) the UI shows helper buttons; for other vendors known
 * to stop background apps aggressively the guide gives the manual path and
 * a link to the vendor page on dontkillmyapp.com; for everything else it
 * says honestly that no extra manufacturer step is known.
 */
data class BackgroundPolicy(
    val label: String,
    val required: Boolean,
    val incompleteDetail: String,
    val confirmedDetail: String,
    val helpUrl: String?,
    val samsungDeviceCare: Boolean,
)

object ManufacturerBackgroundPolicy {
    fun forManufacturer(raw: String): BackgroundPolicy {
        return when (raw.trim().lowercase()) {
            "samsung" -> BackgroundPolicy(
                label = "Samsung",
                required = true,
                incompleteDetail = "Samsung/Galaxy: tap Open Samsung app settings → Battery → " +
                    "Unrestricted, then tap Open Never sleeping apps settings → Background " +
                    "usage limits → Never sleeping apps → add Training Sensors (manual path: " +
                    "Settings → Battery and device care → Battery → Background usage limits); " +
                    "then confirm both steps below",
                confirmedDetail = "Samsung Unrestricted/Never sleeping setup confirmed by user",
                helpUrl = "https://dontkillmyapp.com/samsung",
                samsungDeviceCare = true,
            )
            "xiaomi", "redmi", "poco" -> aggressive(
                "Xiaomi/Redmi/POCO",
                "Security app → Autostart → allow Training Sensors; Settings → Apps → " +
                    "Manage apps → Training Sensors → Battery saver → No restrictions",
                "xiaomi",
            )
            "huawei", "honor" -> aggressive(
                "Huawei/Honor",
                "Settings → Battery → App launch → Training Sensors → Manage manually → " +
                    "allow Auto-launch, Secondary launch and Run in background",
                "huawei",
            )
            "oppo", "realme" -> aggressive(
                "Oppo/realme",
                "Settings → Battery → App battery management → Training Sensors → Allow " +
                    "background activity; also allow Auto-start under App management",
                "oppo",
            )
            "oneplus" -> aggressive(
                "OnePlus",
                "Settings → Battery → Battery optimization → Training Sensors → Don't " +
                    "optimize; disable Advanced/Deep optimization if present",
                "oneplus",
            )
            "vivo", "iqoo" -> aggressive(
                "vivo/iQOO",
                "Settings → Battery → High background power consumption → allow Training " +
                    "Sensors; enable Auto-start in iManager",
                "vivo",
            )
            "meizu" -> aggressive(
                "Meizu",
                "Security app → Permissions → Background management → Training Sensors → " +
                    "Keep running in background",
                "meizu",
            )
            else -> BackgroundPolicy(
                label = raw.trim().ifBlank { "this manufacturer" },
                required = false,
                incompleteDetail = "",
                confirmedDetail = "No additional manufacturer background step is known for " +
                    "this phone; the battery steps above are sufficient",
                helpUrl = null,
                samsungDeviceCare = false,
            )
        }
    }

    private fun aggressive(label: String, path: String, slug: String) = BackgroundPolicy(
        label = label,
        required = true,
        incompleteDetail = "$label phones stop background apps aggressively. $path. " +
            "Tap Open background-settings instructions for detailed steps, then confirm below",
        confirmedDetail = "$label background setup confirmed by user",
        helpUrl = "https://dontkillmyapp.com/$slug",
        samsungDeviceCare = false,
    )
}
