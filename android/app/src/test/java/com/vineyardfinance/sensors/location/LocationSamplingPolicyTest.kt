package com.vineyardfinance.sensors.location

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class LocationSamplingPolicyTest {
    private fun stillWindow(policy: LocationSamplingPolicy, startMs: Long): LocationSamplingPolicy.Transition? {
        var transition: LocationSamplingPolicy.Transition? = null
        for (offset in 0..LocationSamplingPolicy.ACCEL_WINDOW_MS step 500L) {
            transition = policy.onAccelSample(startMs + offset, 9.81) ?: transition
        }
        return transition
    }

    private fun noisyWindow(policy: LocationSamplingPolicy, startMs: Long): LocationSamplingPolicy.Transition? {
        var transition: LocationSamplingPolicy.Transition? = null
        var toggle = false
        for (offset in 0..LocationSamplingPolicy.ACCEL_WINDOW_MS step 500L) {
            toggle = !toggle
            transition = policy.onAccelSample(startMs + offset, if (toggle) 12.5 else 7.0) ?: transition
        }
        return transition
    }

    @Test fun startsMovingAndSettlesToStationaryAfterStillHold() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        assertEquals(LocationSamplingPolicy.Mode.MOVING, policy.mode)
        var transition: LocationSamplingPolicy.Transition? = null
        var windowStart = 0L
        while (transition == null && windowStart < 10 * LocationSamplingPolicy.STILL_HOLD_MS) {
            transition = stillWindow(policy, windowStart)
            windowStart += LocationSamplingPolicy.ACCEL_WINDOW_MS + 500
        }
        val settled = requireNotNull(transition)
        assertEquals(LocationSamplingPolicy.Mode.MOVING, settled.from)
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, settled.to)
        assertTrue(windowStart >= LocationSamplingPolicy.STILL_HOLD_MS)
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, policy.mode)
        // Further still windows do not repeat the transition.
        assertNull(stillWindow(policy, windowStart))
    }

    @Test fun accelMotionPromotesToMovingImmediately() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        var windowStart = 0L
        while (policy.mode == LocationSamplingPolicy.Mode.MOVING) {
            stillWindow(policy, windowStart)
            windowStart += LocationSamplingPolicy.ACCEL_WINDOW_MS + 500
        }
        val transition = requireNotNull(noisyWindow(policy, windowStart))
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, transition.from)
        assertEquals(LocationSamplingPolicy.Mode.MOVING, transition.to)
        assertTrue(transition.reason, transition.reason.startsWith("accelerometer stddev"))
        assertEquals(LocationSamplingPolicy.Mode.MOVING, policy.mode)
    }

    @Test fun dopplerSpeedPromotesToMovingAndSlowSpeedDoesNot() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        var windowStart = 0L
        while (policy.mode == LocationSamplingPolicy.Mode.MOVING) {
            stillWindow(policy, windowStart)
            windowStart += LocationSamplingPolicy.ACCEL_WINDOW_MS + 500
        }
        assertNull(policy.onSpeedSample(windowStart, 0.3))
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, policy.mode)
        val transition = requireNotNull(policy.onSpeedSample(windowStart + 1, 2.8))
        assertEquals(LocationSamplingPolicy.Mode.MOVING, transition.to)
        assertTrue(transition.reason, transition.reason.startsWith("doppler speed"))
    }

    @Test fun motionEvidenceKeepsMovingThroughShortStillness() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        assertNull(policy.onSpeedSample(1_000, 3.0))
        // Still windows shorter than the hold never drop out of MOVING.
        var windowStart = 2_000L
        while (windowStart < 1_000 + LocationSamplingPolicy.STILL_HOLD_MS - LocationSamplingPolicy.ACCEL_WINDOW_MS - 1_000) {
            assertNull(stillWindow(policy, windowStart))
            windowStart += LocationSamplingPolicy.ACCEL_WINDOW_MS + 500
        }
        assertEquals(LocationSamplingPolicy.Mode.MOVING, policy.mode)
    }

    @Test fun clockTickSettlesASignalFreePhone() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        assertNull(policy.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS - 1))
        val transition = requireNotNull(policy.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS))
        assertEquals(LocationSamplingPolicy.Mode.STATIONARY, transition.to)
        assertNull(policy.onClockTick(LocationSamplingPolicy.STILL_HOLD_MS + 1))
    }

    @Test fun accelWindowAccumulatesUntilDurationElapses() {
        val policy = LocationSamplingPolicy(startElapsedMs = 0)
        assertNull(policy.onAccelSample(100, 15.0))
        assertNull(policy.onAccelSample(LocationSamplingPolicy.ACCEL_WINDOW_MS - 1, 2.0))
        // Window completion evaluates the accumulated samples.
        assertNull(policy.onAccelSample(LocationSamplingPolicy.ACCEL_WINDOW_MS, 15.0))
        assertEquals(LocationSamplingPolicy.Mode.MOVING, policy.mode)
    }

    @Test fun standardDeviationHandlesDegenerateInput() {
        assertEquals(0.0, LocationSamplingPolicy.standardDeviation(emptyList()), 0.0)
        assertEquals(0.0, LocationSamplingPolicy.standardDeviation(listOf(9.81)), 0.0)
        assertEquals(1.0, LocationSamplingPolicy.standardDeviation(listOf(9.0, 11.0)), 1e-9)
    }

    @Test fun modeParametersMatchTheProposal() {
        assertEquals(1_000L, LocationSamplingPolicy.Mode.MOVING.locationIntervalMs)
        assertEquals(180_000L, LocationSamplingPolicy.Mode.STATIONARY.locationIntervalMs)
        assertTrue(LocationSamplingPolicy.Mode.STATIONARY.locationIntervalMs in 120_000L..300_000L)
        assertEquals("moving", LocationSamplingPolicy.Mode.MOVING.wireName)
        assertEquals("stationary", LocationSamplingPolicy.Mode.STATIONARY.wireName)
    }
}
