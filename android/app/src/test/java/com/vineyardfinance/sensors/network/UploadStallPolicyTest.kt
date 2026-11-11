package com.vineyardfinance.sensors.network

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class UploadStallPolicyTest {
    private val stall = UploadStallPolicy.STALL_AFTER_MS

    private fun snapshot(
        loopLaunched: Long = 0,
        attemptStarted: Long = 0,
        outcome: Long = 0,
        lastAck: Long = 0,
        lastOutcome: String? = null,
        backoff: Long = 0,
        waitingReconnect: Boolean = false,
        failures: Int = 0,
    ) = UploadLoopVitals.Snapshot(
        loopLaunchedAtMs = loopLaunched,
        iterationAtMs = 0,
        attemptStartedAtMs = attemptStarted,
        outcomeAtMs = outcome,
        lastAckAtMs = lastAck,
        lastOutcome = lastOutcome,
        backoffMs = backoff,
        waitingForReconnect = waitingReconnect,
        consecutiveFailures = failures,
    )

    @Test fun emptyQueueNeverStalls() {
        assertNull(UploadStallPolicy.evaluate(stall * 10, snapshot(loopLaunched = 1), queuedRecords = 0))
    }

    @Test fun unreadableBacklogStaysBlindInsteadOfTriggerHappy() {
        assertNull(UploadStallPolicy.evaluate(stall * 10, snapshot(loopLaunched = 1), queuedRecords = -1))
    }

    @Test fun freshAttemptsAreHealthyEvenWhenServerIsDown() {
        // Failing attempts every <=31s keep attemptStarted fresh: retrying loudly is correct.
        val vitals = snapshot(loopLaunched = 1, attemptStarted = stall * 2, outcome = stall * 2, failures = 40)
        assertNull(UploadStallPolicy.evaluate(stall * 2 + 31_000, vitals, queuedRecords = 100))
    }

    @Test fun launchGraceCountsAsProgress() {
        val vitals = snapshot(loopLaunched = 1_000)
        assertNull(UploadStallPolicy.evaluate(1_000 + stall - 1, vitals, queuedRecords = 100))
        assertNotNull(UploadStallPolicy.evaluate(1_000 + stall, vitals, queuedRecords = 100))
    }

    @Test fun neverLaunchedLoopIsNotJudged() {
        assertNull(UploadStallPolicy.evaluate(stall * 10, snapshot(), queuedRecords = 100))
    }

    @Test fun reconnectWaitNeverStalls() {
        val vitals = snapshot(loopLaunched = 1, waitingReconnect = true)
        assertNull(UploadStallPolicy.evaluate(stall * 10, vitals, queuedRecords = 100))
    }

    @Test fun hungAttemptWithoutOutcomeStalls() {
        val startedAt = 60_000L
        val vitals = snapshot(loopLaunched = 1, attemptStarted = startedAt, lastAck = 30_000, lastOutcome = "durable ack", backoff = 250)
        val now = startedAt + stall
        val report = UploadStallPolicy.evaluate(now, vitals, queuedRecords = 42)
        assertNotNull(report)
        assertEquals(42, report!!.queuedRecords)
        assertEquals(stall, report.progressAgeMs)
        assertEquals(stall, report.attemptAgeMs)
        assertEquals(now - 30_000, report.ackAgeMs)
        assertEquals(250, report.backoffMs)
        assertEquals("durable ack", report.lastOutcome)
    }

    @Test fun silentLoopWithQueuedWorkStalls() {
        val vitals = snapshot(loopLaunched = 1, attemptStarted = 5_000, outcome = 6_000, failures = 3, lastOutcome = "timeout")
        val report = UploadStallPolicy.evaluate(6_000 + stall, vitals, queuedRecords = 7)
        assertNotNull(report)
        assertEquals(3, report!!.consecutiveFailures)
        assertEquals(-1, report.ackAgeMs)
    }

    @Test fun recentOutcomeCountsAsProgressEvenWithOldAttemptStart() {
        val vitals = snapshot(loopLaunched = 1, attemptStarted = 5_000, outcome = 400_000)
        assertNull(UploadStallPolicy.evaluate(400_000 + stall - 1, vitals, queuedRecords = 7))
    }

    @Test fun loopSleepingItsBackoffDuringAnOutageIsNotStalled() {
        val backoff = IngestUploader.MAX_BACKOFF_MS
        val vitals = snapshot(loopLaunched = 1, attemptStarted = 1_000, outcome = 2_000, backoff = backoff, failures = 30)
        assertNull(UploadStallPolicy.evaluate(2_000 + backoff + stall - 1, vitals, queuedRecords = 7))
        assertNotNull(UploadStallPolicy.evaluate(2_000 + backoff + stall, vitals, queuedRecords = 7))
    }
}
