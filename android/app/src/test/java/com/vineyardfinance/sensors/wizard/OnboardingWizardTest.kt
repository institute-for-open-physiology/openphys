package com.vineyardfinance.sensors.wizard

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class OnboardingWizardTest {
    private val openGate = WizardGate(
        sensorTargetValid = true,
        batterySetupComplete = true,
        heartbeatConfirmed = true,
    )
    private val closedGate = WizardGate(
        sensorTargetValid = false,
        batterySetupComplete = false,
        heartbeatConfirmed = false,
    )

    @Test fun stepsFollowTheAgreedOrderWithBatteryBeforeHeartbeat() {
        assertEquals(
            listOf(
                WizardStep.NO_OTHER_APP,
                WizardStep.WET_THE_PADS,
                WizardStep.PUT_IT_ON,
                WizardStep.FIND_SENSOR,
                WizardStep.KEEP_ALIVE,
                WizardStep.SEE_HEARTBEAT,
                WizardStep.RANGE_RULES,
                WizardStep.DONE,
            ),
            WizardStep.ordered(),
        )
    }

    @Test fun copyCarriesTheLoadBearingFacts() {
        assertTrue(WizardStep.NO_OTHER_APP.body.contains("Don't pair the sensor"))
        assertTrue(WizardStep.WET_THE_PADS.body.contains("tap water"))
        assertTrue(WizardStep.PUT_IT_ON.body.contains("below the chest muscles"))
        assertTrue(WizardStep.FIND_SENSOR.body.contains("only wakes up"))
        assertTrue(WizardStep.KEEP_ALIVE.body.contains("Data Saver"))
        assertTrue(WizardStep.RANGE_RULES.body.contains("10 metres"))
        assertTrue(WizardStep.RANGE_RULES.body.contains("No internet is fine"))
        assertTrue(WizardStep.RANGE_RULES.body.contains("lost"))
        assertTrue(WizardStep.DONE.body.contains("Tell us"))
        // No-photos v1: every step is text-only by construction (no image
        // fields exist on WizardStep) — this test documents the decision.
    }

    @Test fun backIsAlwaysAvailableAndNextRespectsPerStepGates() {
        var state = WizardState()
        assertTrue(state.isFirst)
        assertEquals("Step 1 of 8", state.progressLabel)
        assertEquals(state, state.back())

        // Free steps advance even with everything closed.
        state = state.next(closedGate)
        state = state.next(closedGate)
        state = state.next(closedGate)
        assertEquals(WizardStep.FIND_SENSOR, state.step)
        // Gated: no sensor selected, stays put with an honest explanation.
        assertEquals(state, state.next(closedGate))
        assertEquals(
            "Select your sensor from the list, or type the ID printed on it",
            closedGate.blockingDetail(WizardStep.FIND_SENSOR),
        )
        state = state.next(openGate)
        assertEquals(WizardStep.KEEP_ALIVE, state.step)
        assertEquals(state, state.next(closedGate))
        state = state.next(openGate)
        assertEquals(WizardStep.SEE_HEARTBEAT, state.step)
        assertEquals(state, state.next(closedGate))
        state = state.next(openGate)
        assertEquals(WizardStep.RANGE_RULES, state.step)
        assertNull(openGate.blockingDetail(WizardStep.RANGE_RULES))
        state = state.next(openGate)
        assertTrue(state.isLast)
        assertEquals(state, state.next(openGate))

        // Back walks the whole way home.
        repeat(WizardStep.ordered().size) { state = state.back() }
        assertTrue(state.isFirst)
    }

    @Test fun scanListDedupesPrunesAndOrdersByStrength() {
        var sensors = emptyList<DiscoveredSensor>()
        sensors = SensorScanList.upsert(sensors, "Polar H10 AABBCC11", -70, 1_000)
        sensors = SensorScanList.upsert(sensors, "Polar H10 DDEEFF22", -55, 2_000)
        sensors = SensorScanList.upsert(sensors, "Polar H10 AABBCC11", -50, 3_000)
        assertEquals(listOf("AABBCC11", "DDEEFF22"), sensors.map { it.serial })
        assertEquals(-50, sensors.first().rssi)

        // Irrelevant advertisements never enter and never crash.
        sensors = SensorScanList.upsert(sensors, "Polar Verity 123456", -40, 3_500)
        sensors = SensorScanList.upsert(sensors, null, -40, 3_500)
        sensors = SensorScanList.upsert(sensors, "Polar H10 !!", -40, 3_500)
        assertEquals(2, sensors.size)

        // A sensor that stopped advertising ages out.
        sensors = SensorScanList.upsert(sensors, "Polar H10 DDEEFF22", -60, 20_000)
        assertEquals(listOf("DDEEFF22"), sensors.map { it.serial })
        assertFalse(sensors.any { it.serial == "AABBCC11" })
    }

    @Test fun eligibleSerialNormalizesThroughTheSharedIdentityRules() {
        assertEquals("AABBCC11", SensorScanList.eligibleSerial("Polar H10 AABBCC11"))
        assertNull(SensorScanList.eligibleSerial("Polar Verity AABBCC11"))
        assertNull(SensorScanList.eligibleSerial(null))
        assertNull(SensorScanList.eligibleSerial("Polar H10 x"))
    }

    @Test fun signalLabelsAreMonotonic() {
        assertEquals("strong signal", SensorScanList.signalLabel(-50))
        assertEquals("good signal", SensorScanList.signalLabel(-70))
        assertEquals("weak signal", SensorScanList.signalLabel(-85))
        assertEquals("very weak signal", SensorScanList.signalLabel(-95))
    }
}
