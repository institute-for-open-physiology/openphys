package com.vineyardfinance.sensors.wizard

import com.vineyardfinance.sensors.collector.H10Identity

/**
 * Pure aggregation for the wizard's live nearby-sensor list. The Android
 * scanner boundary stays thin: it feeds advertised names and RSSI readings
 * in; this object owns dedupe-by-serial, freshest-signal wins, strongest
 * first ordering, and pruning of sensors that stopped advertising.
 */
data class DiscoveredSensor(
    val serial: String,
    val rssi: Int,
    val lastSeenUnixMs: Long,
)

object SensorScanList {
    const val STALE_AFTER_MS: Long = 15_000
    private const val ADVERTISED_PREFIX = "Polar H10 "

    /** Returns the serial for a wizard-eligible advertisement, else null. */
    fun eligibleSerial(advertisedName: String?): String? {
        if (advertisedName == null || !advertisedName.startsWith(ADVERTISED_PREFIX)) return null
        return runCatching { H10Identity.configuredSerial(advertisedName) }.getOrNull()
    }

    fun upsert(
        sensors: List<DiscoveredSensor>,
        advertisedName: String?,
        rssi: Int,
        nowUnixMs: Long,
    ): List<DiscoveredSensor> {
        val serial = eligibleSerial(advertisedName) ?: return prune(sensors, nowUnixMs)
        val updated = sensors.filterNot { it.serial == serial } +
            DiscoveredSensor(serial, rssi, nowUnixMs)
        return prune(updated, nowUnixMs)
    }

    fun prune(sensors: List<DiscoveredSensor>, nowUnixMs: Long): List<DiscoveredSensor> =
        sensors
            .filter { nowUnixMs - it.lastSeenUnixMs <= STALE_AFTER_MS }
            .sortedByDescending { it.rssi }

    /** Human signal description for the list row. */
    fun signalLabel(rssi: Int): String = when {
        rssi >= -60 -> "strong signal"
        rssi >= -75 -> "good signal"
        rssi >= -90 -> "weak signal"
        else -> "very weak signal"
    }
}
