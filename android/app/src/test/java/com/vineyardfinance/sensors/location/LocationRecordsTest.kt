package com.vineyardfinance.sensors.location

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LocationRecordsTest {
    private val fullFix = FixSample(
        provider = "gps",
        latitudeDeg = 55.9445,
        longitudeDeg = -3.1892,
        altitudeM = 102.4,
        horizontalAccuracyM = 3.9,
        verticalAccuracyM = 5.1,
        speedMps = 3.42,
        speedAccuracyMps = 0.4,
        bearingDeg = 271.5,
        bearingAccuracyDeg = 12.0,
        deviceTimeUnixMs = 1_770_000_000_123L,
        deviceElapsedRealtimeNs = 987_654_321_000L,
        isMock = false,
    )

    @Test fun locationFixBodyEncodesEveryRawField() {
        val body = LocationRecords.locationFixBody(fullFix, "moving")
        assertEquals("gps", body.get("provider").asString)
        assertEquals("moving", body.get("sampling_mode").asString)
        assertEquals(55.9445, body.get("latitude_deg").asDouble, 0.0)
        assertEquals(-3.1892, body.get("longitude_deg").asDouble, 0.0)
        assertEquals(102.4, body.get("altitude_m").asDouble, 0.0)
        assertEquals(3.9, body.get("horizontal_accuracy_m").asDouble, 0.0)
        assertEquals(5.1, body.get("vertical_accuracy_m").asDouble, 0.0)
        assertEquals(3.42, body.get("speed_mps").asDouble, 0.0)
        assertEquals(0.4, body.get("speed_accuracy_mps").asDouble, 0.0)
        assertEquals(271.5, body.get("bearing_deg").asDouble, 0.0)
        assertEquals(12.0, body.get("bearing_accuracy_deg").asDouble, 0.0)
        assertEquals(1_770_000_000_123L, body.get("device_time_unix_ms").asLong)
        assertEquals(987_654_321_000L, body.get("device_elapsed_realtime_ns").asLong)
        assertEquals(false, body.get("is_mock").asBoolean)
    }

    @Test fun locationFixBodyKeepsAbsentPlatformFieldsAsExplicitNulls() {
        val sparse = fullFix.copy(
            provider = "fused",
            altitudeM = null,
            horizontalAccuracyM = null,
            verticalAccuracyM = null,
            speedMps = null,
            speedAccuracyMps = null,
            bearingDeg = null,
            bearingAccuracyDeg = null,
            isMock = true,
        )
        val body = LocationRecords.locationFixBody(sparse, "stationary")
        for (field in listOf(
            "altitude_m", "horizontal_accuracy_m", "vertical_accuracy_m", "speed_mps",
            "speed_accuracy_mps", "bearing_deg", "bearing_accuracy_deg",
        )) {
            assertTrue(field, body.has(field))
            assertTrue(field, body.get(field).isJsonNull)
        }
        assertEquals("fused", body.get("provider").asString)
        assertEquals("stationary", body.get("sampling_mode").asString)
        assertEquals(true, body.get("is_mock").asBoolean)
    }

    @Test fun gnssStatusBodySummarizesUsedSatellites() {
        val body = LocationRecords.gnssStatusBody(
            GnssStatusSample(satellitesVisible = 21, usedCn0DbHz = listOf(30.0, 35.0, 43.0)),
        )
        assertEquals(21, body.get("satellites_visible").asInt)
        assertEquals(3, body.get("satellites_used").asInt)
        assertEquals(36.0, body.get("cn0_used_mean_db_hz").asDouble, 1e-9)
        assertEquals(43.0, body.get("cn0_used_max_db_hz").asDouble, 0.0)
    }

    @Test fun gnssStatusBodyWithNoUsedSatellitesReportsNullSummary() {
        val body = LocationRecords.gnssStatusBody(GnssStatusSample(4, emptyList()))
        assertEquals(4, body.get("satellites_visible").asInt)
        assertEquals(0, body.get("satellites_used").asInt)
        assertTrue(body.get("cn0_used_mean_db_hz").isJsonNull)
        assertTrue(body.get("cn0_used_max_db_hz").isJsonNull)
    }

    @Test fun pressureSampleBodyEncodesRawSensorFields() {
        val body = LocationRecords.pressureSampleBody(
            PressureSample(pressureHpa = 1_006.32, sensorAccuracy = 3, sensorTimestampNs = 5_000L),
            "moving",
        )
        assertEquals(1_006.32, body.get("pressure_hpa").asDouble, 0.0)
        assertEquals(3, body.get("sensor_accuracy").asInt)
        assertEquals(5_000L, body.get("sensor_timestamp_ns").asLong)
        assertEquals("moving", body.get("sampling_mode").asString)
    }
}
