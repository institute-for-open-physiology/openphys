package com.vineyardfinance.sensors.location

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class LocationRecorderTest {
    private var consent = true

    private fun recorder(startMs: Long = 0): LocationRecorder =
        LocationRecorder({ consent }, LocationSamplingPolicy(startMs), startMs)

    private fun fix(speedMps: Double? = 3.0) = FixSample(
        provider = "gps",
        latitudeDeg = 55.9,
        longitudeDeg = -3.1,
        altitudeM = 100.0,
        horizontalAccuracyM = 4.0,
        verticalAccuracyM = null,
        speedMps = speedMps,
        speedAccuracyMps = null,
        bearingDeg = null,
        bearingAccuracyDeg = null,
        deviceTimeUnixMs = 1_770_000_000_000L,
        deviceElapsedRealtimeNs = 1_000L,
        isMock = false,
    )

    private fun gnss() = GnssStatusSample(10, listOf(30.0, 40.0))
    private fun pressure() = PressureSample(1_001.0, 3, 2_000L)

    @Test fun disabledOptInProducesZeroLocationRecordsEvenWhileTheCollectorRuns() {
        // The collector keeps running for H10 data; every location signal kind
        // keeps flowing, and none may become a record while the toggle is off.
        consent = false
        val recorder = recorder()
        var elapsed = 0L
        repeat(200) {
            elapsed += 1_000
            val fixOutput = recorder.onFix(elapsed, fix())
            assertNull(fixOutput.kind)
            assertNull(fixOutput.body)
            val gnssOutput = recorder.onGnssStatus(elapsed, gnss())
            assertNull(gnssOutput.kind)
            assertNull(gnssOutput.body)
            val pressureOutput = recorder.onPressure(elapsed, pressure())
            assertNull(pressureOutput.kind)
            assertNull(pressureOutput.body)
            assertNull(recorder.fixWatchdog(elapsed + 1_000_000))
        }
    }

    @Test fun revokingConsentMidStreamStopsRecordsImmediately() {
        val recorder = recorder()
        assertEquals(LocationRecords.LOCATION_FIX, recorder.onFix(1_000, fix()).kind)
        consent = false
        assertNull(recorder.onFix(2_000, fix()).kind)
        assertNull(recorder.onGnssStatus(60_000, gnss()).kind)
        assertNull(recorder.onPressure(60_000, pressure()).kind)
        consent = true
        assertEquals(LocationRecords.LOCATION_FIX, recorder.onFix(3_000, fix()).kind)
    }

    @Test fun fixRecordCarriesTheCurrentSamplingMode() {
        val recorder = recorder()
        val output = recorder.onFix(1_000, fix(speedMps = 3.0))
        assertEquals(LocationRecords.LOCATION_FIX, output.kind)
        assertEquals("moving", requireNotNull(output.body).get("sampling_mode").asString)
        assertNull(output.transition)
    }

    @Test fun fastFixPromotesAStationaryRecorderAndSurfacesTheTransition() {
        val recorder = recorder()
        assertNotNull(recorder.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS))
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, recorder.mode)
        val output = recorder.onFix(LocationSamplingPolicy.STILL_HOLD_MS + 1_000, fix(speedMps = 2.5))
        assertEquals(LocationSamplingPolicy.Mode.MOVING, requireNotNull(output.transition).to)
        assertEquals("moving", requireNotNull(output.body).get("sampling_mode").asString)
    }

    @Test fun gnssStatusIsThrottledToItsLowRateInterval() {
        val recorder = recorder()
        assertEquals(LocationRecords.GNSS_STATUS, recorder.onGnssStatus(0, gnss()).kind)
        assertNull(recorder.onGnssStatus(LocationRecorder.GNSS_STATUS_INTERVAL_MS - 1, gnss()).kind)
        assertEquals(
            LocationRecords.GNSS_STATUS,
            recorder.onGnssStatus(LocationRecorder.GNSS_STATUS_INTERVAL_MS, gnss()).kind,
        )
    }

    @Test fun pressureThrottleFollowsTheSamplingMode() {
        val recorder = recorder()
        assertEquals(LocationRecords.PRESSURE_SAMPLE, recorder.onPressure(0, pressure()).kind)
        assertNull(recorder.onPressure(500, pressure()).kind)
        assertEquals(LocationRecords.PRESSURE_SAMPLE, recorder.onPressure(1_000, pressure()).kind)
        // Settle to stationary: barometer drops to its low rate.
        assertNotNull(recorder.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS))
        val base = LocationSamplingPolicy.STILL_HOLD_MS
        assertEquals(LocationRecords.PRESSURE_SAMPLE, recorder.onPressure(base, pressure()).kind)
        assertNull(recorder.onPressure(base + 30_000, pressure()).kind)
        val output = recorder.onPressure(base + LocationSamplingPolicy.Mode.STATIONARY.pressureIntervalMs, pressure())
        assertEquals(LocationRecords.PRESSURE_SAMPLE, output.kind)
        assertEquals("stationary", requireNotNull(output.body).get("sampling_mode").asString)
    }

    @Test fun fixWatchdogFiresOncePerStallAndResetsOnTheNextFix() {
        val recorder = recorder()
        recorder.onFix(0, fix())
        val stallAt = LocationSamplingPolicy.Mode.MOVING.fixStallAfterMs
        assertNull(recorder.fixWatchdog(stallAt - 1))
        val detail = requireNotNull(recorder.fixWatchdog(stallAt))
        assertEquals(true, detail.contains("moving"))
        // Loud once, then quiet until fixes resume and stall again.
        assertNull(recorder.fixWatchdog(stallAt + 60_000))
        recorder.onFix(stallAt + 61_000, fix())
        assertNull(recorder.fixWatchdog(stallAt + 62_000))
        assertNotNull(recorder.fixWatchdog(stallAt + 61_000 + LocationSamplingPolicy.Mode.MOVING.fixStallAfterMs))
    }

    @Test fun stationaryFixWatchdogUsesTheLongerLimit() {
        val recorder = recorder()
        assertNotNull(recorder.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS))
        recorder.onFix(LocationSamplingPolicy.STILL_HOLD_MS, fix(speedMps = 0.0))
        val base = LocationSamplingPolicy.STILL_HOLD_MS
        assertNull(recorder.fixWatchdog(base + LocationSamplingPolicy.Mode.MOVING.fixStallAfterMs))
        assertNotNull(recorder.fixWatchdog(base + LocationSamplingPolicy.Mode.STATIONARY.fixStallAfterMs))
    }

    @Test fun accelInputDrivesTheModeWithoutProducingRecords() {
        val recorder = recorder()
        var transition: LocationSamplingPolicy.Transition? = null
        var elapsed = 0L
        while (transition == null && elapsed < 10 * LocationSamplingPolicy.STILL_HOLD_MS) {
            elapsed += 1_000
            transition = recorder.onAccel(elapsed, 9.81)
        }
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, requireNotNull(transition).to)
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, recorder.mode)
    }
}
