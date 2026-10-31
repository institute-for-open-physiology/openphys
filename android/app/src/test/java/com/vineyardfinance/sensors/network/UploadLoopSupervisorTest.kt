package com.vineyardfinance.sensors.network

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException

class UploadLoopSupervisorTest {
    private val stall = UploadStallPolicy.STALL_AFTER_MS

    // Production clocks are unix millis or elapsedRealtime and never zero; the policy reads
    // timestamp 0 as "never happened", so the virtual clock needs a nonzero epoch.
    private val EPOCH_MS = 1_700_000_000_000L

    @Test fun restartsSilentlyStalledLoopAndReportsIt() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        val stalls = mutableListOf<UploadStallPolicy.Report>()
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 42L },
            onStall = { stalls.add(it) },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
                awaitCancellation()
            }
        }

        advanceTimeBy(stall + 1_001)
        runCurrent()
        assertEquals(1, stalls.size)
        assertEquals(42L, stalls[0].queuedRecords)
        assertTrue(stalls[0].progressAgeMs >= stall)
        assertEquals(2, launches)

        advanceTimeBy(stall + 1_001)
        runCurrent()
        assertEquals(2, stalls.size)
        assertEquals(3, launches)

        active = false
        advanceTimeBy(1_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }

    @Test fun loopExitAfterCollectionDisabledEndsSupervision() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var launches = 0
        var stalls = 0
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 0L },
            onStall = { stalls += 1 },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )

        supervisor.supervise({ launches == 0 }) {
            launches += 1
            vitals.loopLaunched()
        }

        assertEquals(1, launches)
        assertEquals(0, stalls)
    }

    @Test fun loopExitWhileCollectionEnabledIsRelaunchedNotSilentlyDropped() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        val failures = mutableListOf<String>()
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 0L },
            onStall = {},
            onLoopFailure = { failures.add(it) },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
            }
        }

        advanceTimeBy(4_500)
        runCurrent()
        assertTrue("expected relaunches, got $launches", launches >= 2)
        assertTrue(failures.any { it.contains("exited while collection remained enabled") })

        active = false
        advanceTimeBy(2_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }

    @Test fun abnormallyDyingLoopIsReportedAndRelaunchedAfterAPause() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        val failures = mutableListOf<String>()
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 5L },
            onStall = {},
            onLoopFailure = { failures.add(it) },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
                if (launches <= 2) throw IOException("dispatcher wedge")
                awaitCancellation()
            }
        }

        // Death #2 is joined at t=3000; the anti-hot-loop pause defers relaunch #3 to t=4000.
        advanceTimeBy(3_501)
        runCurrent()
        assertEquals(2, failures.size)
        assertTrue(failures[0].contains("dispatcher wedge"))
        assertEquals(2, launches)

        advanceTimeBy(600)
        runCurrent()
        assertEquals(3, launches)

        active = false
        advanceTimeBy(1_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }

    @Test fun loopThatIgnoresCancellationIsAbandonedAndReplaced() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        val failures = mutableListOf<String>()
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 42L },
            onStall = {},
            onLoopFailure = { failures.add(it) },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
            joinTimeoutMs = 2_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
                if (launches == 1) {
                    try {
                        awaitCancellation()
                    } catch (cancelled: CancellationException) {
                        // Simulate an await cancellation cannot break (blocking IO).
                        withContext(NonCancellable) { delay(60_000) }
                        throw cancelled
                    }
                } else {
                    awaitCancellation()
                }
            }
        }

        // Stall fires at t=300_000; the stuck loop ignores the cancel, so the bounded join
        // gives up at t=302_000 and a replacement must start beside it.
        advanceTimeBy(stall + 2_001 + 1_001)
        runCurrent()
        assertEquals(2, launches)
        assertTrue(failures.any { it.contains("abandoning") })

        active = false
        advanceTimeBy(61_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }

    @Test fun reconnectWaitIsNeverTreatedAsAStall() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var stalls = 0
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 42L },
            onStall = { stalls += 1 },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ true }) {
                vitals.loopLaunched()
                vitals.reconnectWait(true)
                awaitCancellation()
            }
        }

        advanceTimeBy(stall * 3)
        runCurrent()
        assertEquals(0, stalls)
        job.cancel()
    }

    @Test fun backlogReadFailureLeavesWatchdogBlindNotTriggerHappy() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var stalls = 0
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { throw IOException("database busy") },
            onStall = { stalls += 1 },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ true }) {
                vitals.loopLaunched()
                awaitCancellation()
            }
        }

        advanceTimeBy(stall * 3)
        runCurrent()
        assertEquals(0, stalls)
        job.cancel()
    }

    @Test fun hungBacklogReadLeavesWatchdogBlindWithoutWedgingIt() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var stalls = 0
        var launches = 0
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { awaitCancellation() },
            onStall = { stalls += 1 },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
            telemetryTimeoutMs = 2_000,
        )
        val job = launch {
            supervisor.supervise({ true }) {
                launches += 1
                vitals.loopLaunched()
                awaitCancellation()
            }
        }

        advanceTimeBy(stall * 2)
        runCurrent()
        assertEquals(0, stalls)
        assertEquals(1, launches)
        assertFalse(job.isCompleted)
        job.cancel()
    }

    @Test fun telemetryFailuresNeverBlockRecovery() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 42L },
            onStall = { throw IOException("writer failed") },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
                awaitCancellation()
            }
        }

        advanceTimeBy(stall + 1_001)
        runCurrent()
        assertEquals(2, launches)

        active = false
        advanceTimeBy(1_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }

    @Test fun hungStallReportCannotBlockRecovery() = runTest {
        val vitals = UploadLoopVitals { testScheduler.currentTime + EPOCH_MS }
        var launches = 0
        var active = true
        val supervisor = UploadLoopSupervisor(
            vitals = vitals,
            queuedRecords = { 42L },
            onStall = { awaitCancellation() },
            clock = { testScheduler.currentTime + EPOCH_MS },
            checkIntervalMs = 1_000,
            telemetryTimeoutMs = 2_000,
        )
        val job = launch {
            supervisor.supervise({ active }) {
                launches += 1
                vitals.loopLaunched()
                awaitCancellation()
            }
        }

        // The loop is cancelled BEFORE the stall report; a hung report is bounded at
        // telemetryTimeoutMs and the replacement still launches.
        advanceTimeBy(stall + 2_001 + 1_001)
        runCurrent()
        assertEquals(2, launches)

        active = false
        advanceTimeBy(1_001)
        runCurrent()
        assertTrue(job.isCompleted)
    }
}
