package com.vineyardfinance.sensors.setup

data class SetupFacts(
    val credentialAvailable: Boolean,
    val installationIdValid: Boolean,
    val bluetoothAvailable: Boolean,
    val bluetoothEnabled: Boolean,
    val nearbyPermissionsGranted: Boolean,
    val notificationPermissionGranted: Boolean,
    val notificationsEnabled: Boolean,
    val notificationChannelEnabled: Boolean,
    val batteryOptimizationExempt: Boolean,
    val backgroundRestricted: Boolean,
    val manufacturerPolicyRequired: Boolean,
    val manufacturerIncompleteDetail: String,
    val manufacturerConfirmedDetail: String,
    val manufacturerBackgroundConfirmed: Boolean,
    val h10TargetValid: Boolean,
    val collectionEnabled: Boolean,
    val collectorConnected: Boolean,
    val ecgFresh: Boolean,
    val accFresh: Boolean,
    val hrFresh: Boolean,
    val storageHealthy: Boolean,
    val cloudAckFresh: Boolean,
)

data class SetupItem(val name: String, val complete: Boolean, val detail: String)

data class SetupReport(
    val items: List<SetupItem>,
    val preStartComplete: Boolean,
    val operationalComplete: Boolean,
) {
    val complete: Boolean get() = preStartComplete && operationalComplete
    val firstIncomplete: SetupItem? get() = items.firstOrNull { !it.complete }
}

object SetupChecklist {
    fun evaluate(facts: SetupFacts): SetupReport {
        val connected = facts.credentialAvailable && facts.installationIdValid
        val bluetooth = facts.bluetoothAvailable && facts.bluetoothEnabled
        val notifications = facts.notificationPermissionGranted && facts.notificationsEnabled && facts.notificationChannelEnabled
        val battery = facts.batteryOptimizationExempt && !facts.backgroundRestricted
        val manufacturer = !facts.manufacturerPolicyRequired || facts.manufacturerBackgroundConfirmed
        val streams = facts.collectionEnabled && facts.collectorConnected && facts.ecgFresh && facts.accFresh && facts.hrFresh
        val endToEnd = facts.storageHealthy && facts.cloudAckFresh
        val items = listOf(
            SetupItem("Phone connected", connected, if (connected) "Permanent device credential ready" else "Log in with email and password, or connect through the website"),
            SetupItem("Bluetooth on", bluetooth, if (bluetooth) "Bluetooth adapter enabled" else "Turn on Bluetooth"),
            SetupItem("Nearby devices", facts.nearbyPermissionsGranted, if (facts.nearbyPermissionsGranted) "Bluetooth scan and connect allowed" else "Grant Nearby devices access"),
            SetupItem("Notifications", notifications, if (notifications) "Foreground-service notifications allowed" else "Allow notifications and the Polar H10 collection channel"),
            SetupItem("Background battery operation", battery, if (battery) "Battery optimization disabled" else "Allow unrestricted background battery use"),
            SetupItem(
                "Manufacturer background policy",
                manufacturer,
                if (manufacturer) {
                    facts.manufacturerConfirmedDetail
                } else {
                    facts.manufacturerIncompleteDetail
                },
            ),
            SetupItem("H10 readiness", facts.h10TargetValid && streams, when {
                !facts.h10TargetValid -> "Enter the serial printed on the Polar H10"
                !facts.collectionEnabled -> "Start collection after the prerequisites above are complete"
                streams -> "Foreground collector and ECG/ACC/HR streams are fresh"
                else -> "Collector is starting, pairing, or reconnecting"
            }),
            SetupItem("End-to-end health", endToEnd, when {
                !facts.storageHealthy -> "Local durable storage requires attention"
                !facts.cloudAckFresh -> "Waiting for a recent authenticated durable cloud ACK"
                else -> "Local queue healthy and cloud ACK recent"
            }),
        )
        val preStart = connected && bluetooth && facts.nearbyPermissionsGranted && notifications && battery && manufacturer && facts.h10TargetValid
        return SetupReport(items, preStart, streams && endToEnd)
    }
}
