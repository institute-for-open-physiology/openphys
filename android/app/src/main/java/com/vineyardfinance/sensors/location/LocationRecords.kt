package com.vineyardfinance.sensors.location

import com.google.gson.JsonNull
import com.google.gson.JsonObject

/**
 * One raw Android `Location` fix, already unpacked from the platform object so
 * pure logic and host tests never touch Android classes. Fields mirror
 * `android.location.Location` unmodified: `altitudeM` is WGS84 ellipsoid
 * height (not sea level) and `speedMps` is Doppler-derived — the primary
 * running-speed signal (docs/gps_tracking_proposal_20260808.md).
 */
data class FixSample(
    val provider: String,
    val latitudeDeg: Double,
    val longitudeDeg: Double,
    val altitudeM: Double?,
    val horizontalAccuracyM: Double?,
    val verticalAccuracyM: Double?,
    val speedMps: Double?,
    val speedAccuracyMps: Double?,
    val bearingDeg: Double?,
    val bearingAccuracyDeg: Double?,
    val deviceTimeUnixMs: Long,
    val deviceElapsedRealtimeNs: Long,
    val isMock: Boolean,
)

/** Low-rate GNSS quality context so bad fixes stay explainable. */
data class GnssStatusSample(
    val satellitesVisible: Int,
    val usedCn0DbHz: List<Double>,
)

/** Barometric pressure sample; relative elevation reference during runs. */
data class PressureSample(
    val pressureHpa: Double,
    val sensorAccuracy: Int,
    val sensorTimestampNs: Long,
)

/**
 * Record-body encoders for the location record kinds. Field names are frozen
 * against the Rust WAL body structs in `src/wal.rs` (`LocationFixBody`,
 * `GnssStatusBody`, `PressureSampleBody`).
 */
object LocationRecords {
    const val LOCATION_FIX = "location_fix"
    const val GNSS_STATUS = "gnss_status"
    const val PRESSURE_SAMPLE = "pressure_sample"

    fun locationFixBody(fix: FixSample, samplingMode: String): JsonObject = JsonObject().apply {
        addProperty("provider", fix.provider)
        addProperty("sampling_mode", samplingMode)
        addProperty("latitude_deg", fix.latitudeDeg)
        addProperty("longitude_deg", fix.longitudeDeg)
        nullableNumber("altitude_m", fix.altitudeM)
        nullableNumber("horizontal_accuracy_m", fix.horizontalAccuracyM)
        nullableNumber("vertical_accuracy_m", fix.verticalAccuracyM)
        nullableNumber("speed_mps", fix.speedMps)
        nullableNumber("speed_accuracy_mps", fix.speedAccuracyMps)
        nullableNumber("bearing_deg", fix.bearingDeg)
        nullableNumber("bearing_accuracy_deg", fix.bearingAccuracyDeg)
        addProperty("device_time_unix_ms", fix.deviceTimeUnixMs)
        addProperty("device_elapsed_realtime_ns", fix.deviceElapsedRealtimeNs)
        addProperty("is_mock", fix.isMock)
    }

    fun gnssStatusBody(sample: GnssStatusSample): JsonObject = JsonObject().apply {
        addProperty("satellites_visible", sample.satellitesVisible)
        addProperty("satellites_used", sample.usedCn0DbHz.size)
        nullableNumber("cn0_used_mean_db_hz", sample.usedCn0DbHz.takeIf { it.isNotEmpty() }?.let { it.sum() / it.size })
        nullableNumber("cn0_used_max_db_hz", sample.usedCn0DbHz.maxOrNull())
    }

    fun pressureSampleBody(sample: PressureSample, samplingMode: String): JsonObject = JsonObject().apply {
        addProperty("pressure_hpa", sample.pressureHpa)
        addProperty("sensor_accuracy", sample.sensorAccuracy)
        addProperty("sensor_timestamp_ns", sample.sensorTimestampNs)
        addProperty("sampling_mode", samplingMode)
    }

    private fun JsonObject.nullableNumber(name: String, value: Double?) {
        if (value == null) add(name, JsonNull.INSTANCE) else addProperty(name, value)
    }
}
