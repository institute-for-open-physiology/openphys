package com.vineyardfinance.sensors.network

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadLoopVitalsTest {
    private var now = 0L
    private val vitals = UploadLoopVitals { now }

    @Test fun startsWithEverythingUnset() {
        val snapshot = vitals.snapshot()
        assertEquals(0, snapshot.loopLaunchedAtMs)
        assertEquals(0, snapshot.attemptStartedAtMs)
        assertEquals(0, snapshot.lastAckAtMs)
        assertNull(snapshot.lastOutcome)
        assertFalse(snapshot.waitingForReconnect)
        assertEquals(0, snapshot.consecutiveFailures)
    }

    @Test fun recordsAttemptAckAndFailureTransitions() {
        now = 10; vitals.loopLaunched()
        now = 20; vitals.iteration()
        now = 30; vitals.attemptStarted()
        now = 40; vitals.attemptFailed("boom")
        now = 45; vitals.backoff(500)
        var snapshot = vitals.snapshot()
        assertEquals(10, snapshot.loopLaunchedAtMs)
        assertEquals(20, snapshot.iterationAtMs)
        assertEquals(30, snapshot.attemptStartedAtMs)
        assertEquals(40, snapshot.outcomeAtMs)
        assertEquals("boom", snapshot.lastOutcome)
        assertEquals(500, snapshot.backoffMs)
        assertEquals(1, snapshot.consecutiveFailures)

        now = 50; vitals.attemptStarted()
        now = 60; vitals.ackReceived()
        snapshot = vitals.snapshot()
        assertEquals(60, snapshot.outcomeAtMs)
        assertEquals(60, snapshot.lastAckAtMs)
        assertEquals("durable ack", snapshot.lastOutcome)
        assertEquals(0, snapshot.consecutiveFailures)
    }

    @Test fun failureMessagesAreTruncated() {
        vitals.attemptFailed("x".repeat(500))
        assertEquals(160, vitals.snapshot().lastOutcome!!.length)
    }

    @Test fun relaunchClearsStaleReconnectWait() {
        vitals.reconnectWait(true)
        assertTrue(vitals.snapshot().waitingForReconnect)
        vitals.loopLaunched()
        assertFalse(vitals.snapshot().waitingForReconnect)
    }

    @Test fun ageIsNegativeOneForNeverAndClampedAtZero() {
        assertEquals(-1, UploadLoopVitals.ageMs(100, 0))
        assertEquals(-1, UploadLoopVitals.ageMs(100, -5))
        assertEquals(0, UploadLoopVitals.ageMs(100, 200))
        assertEquals(70, UploadLoopVitals.ageMs(100, 30))
    }
}
