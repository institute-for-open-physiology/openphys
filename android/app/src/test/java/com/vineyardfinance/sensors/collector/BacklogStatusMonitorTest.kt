package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.data.BacklogStats
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class BacklogStatusMonitorTest {
    @Test fun processDefaultZeroCannotBeMistakenForAMeasuredEmptyQueue() {
        val initial = CollectorSnapshot()

        assertFalse(initial.backlogKnown)
        assertEquals("queue status checking", initial.backlogDescription())

        val measured = initial.withBacklog(BacklogStats(records = 0, oldestUnixNs = null))
        assertTrue(measured.backlogKnown)
        assertEquals("0 records", measured.backlogDescription())
    }

    @Test fun applicationMonitorPublishesResidualDrainAfterCollectorServiceStops() = runTest {
        val queue = MutableSharedFlow<BacklogStats>(extraBufferCapacity = 2)
        var status = CollectorSnapshot()
        val monitor = BacklogStatusMonitor(
            backlog = queue,
            publish = { status = status.withBacklog(it) },
            publishUnavailable = { status = status.withBacklogUnavailable(it) },
            waitBeforeRetry = { yield() },
        )
        val job = launch(UnconfinedTestDispatcher(testScheduler)) { monitor.run() }

        queue.emit(BacklogStats(records = 7, oldestUnixNs = 1))
        assertEquals(7, status.backlogRecords)
        assertTrue(status.backlogKnown)

        // No service-owned job is involved: the process monitor remains alive while the residual
        // worker acknowledges the durable batch and Room emits its empty aggregate.
        queue.emit(BacklogStats(records = 0, oldestUnixNs = null))
        assertEquals(0, status.backlogRecords)
        assertTrue(status.backlogKnown)
        assertNull(status.backlogStatusError)

        job.cancel()
    }

    @Test fun streamFailureIsVisibleAndRetriesWithoutOverlappingSubscriptions() = runTest {
        var attempts = 0
        var activeSubscriptions = 0
        var maximumActiveSubscriptions = 0
        val failures = mutableListOf<String>()
        var status = CollectorSnapshot()
        val backlog = flow {
            attempts += 1
            activeSubscriptions += 1
            maximumActiveSubscriptions = maxOf(maximumActiveSubscriptions, activeSubscriptions)
            try {
                if (attempts == 1) error("database observer failed")
                emit(BacklogStats(records = 3, oldestUnixNs = 1))
                awaitCancellation()
            } finally {
                activeSubscriptions -= 1
            }
        }
        val monitor = BacklogStatusMonitor(
            backlog = backlog,
            publish = { status = status.withBacklog(it) },
            publishUnavailable = {
                failures += it
                status = status.withBacklogUnavailable(it)
            },
            waitBeforeRetry = { yield() },
        )
        val job = launch(UnconfinedTestDispatcher(testScheduler)) { monitor.run() }

        assertEquals(2, attempts)
        assertEquals(1, maximumActiveSubscriptions)
        assertTrue(failures.single().contains("database observer failed"))
        assertTrue(status.backlogKnown)
        assertEquals(3, status.backlogRecords)
        assertNull(status.backlogStatusError)

        job.cancel()
    }

    @Test fun unexpectedStreamCompletionIsMarkedUnavailableBeforeOneSequentialRetry() = runTest {
        var attempts = 0
        val failures = mutableListOf<String>()
        var status = CollectorSnapshot()
        val backlog = flow {
            attempts += 1
            emit(BacklogStats(records = attempts.toLong(), oldestUnixNs = 1))
            if (attempts > 1) awaitCancellation()
        }
        val monitor = BacklogStatusMonitor(
            backlog = backlog,
            publish = { status = status.withBacklog(it) },
            publishUnavailable = {
                failures += it
                status = status.withBacklogUnavailable(it)
            },
            waitBeforeRetry = { yield() },
        )
        val job = launch(UnconfinedTestDispatcher(testScheduler)) { monitor.run() }

        assertEquals(2, attempts)
        assertEquals(listOf("Local queue status stream ended unexpectedly"), failures)
        assertEquals(2, status.backlogRecords)
        assertTrue(status.backlogKnown)
        assertNull(status.backlogStatusError)

        job.cancel()
    }

    @Test fun unavailableQueueStatusDoesNotOverwriteAnArchiveCriticalFailure() {
        val fatal = CollectorRuntimePolicy.failedStatus(CollectorSnapshot(), "queue insert failed")
        val unavailable = fatal.withBacklogUnavailable("Local queue status unavailable")

        assertEquals("Collection failed", unavailable.state)
        assertEquals("queue insert failed", unavailable.lastError)
        assertEquals("Local queue status unavailable", unavailable.backlogStatusError)
        assertFalse(unavailable.backlogKnown)
    }

    @Test fun cancellationStopsTheMonitorWithoutPublishingAFalseFailure() = runTest {
        val failures = mutableListOf<String>()
        val monitor = BacklogStatusMonitor(
            backlog = flow { throw CancellationException("process ending") },
            publish = {},
            publishUnavailable = failures::add,
            waitBeforeRetry = { error("must not retry cancellation") },
        )

        val failure = runCatching { monitor.run() }.exceptionOrNull()

        assertTrue(failure is CancellationException)
        assertTrue(failures.isEmpty())
    }

    @Test fun vmAndLinkageErrorsAreNotAbsorbedByTheRetryLoop() = runTest {
        val failures = mutableListOf<String>()
        val monitor = BacklogStatusMonitor(
            backlog = flow { throw LinkageError("broken runtime linkage") },
            publish = {},
            publishUnavailable = failures::add,
            waitBeforeRetry = { error("must not retry a VM/linkage error") },
        )

        val failure = runCatching { monitor.run() }.exceptionOrNull()

        assertTrue(failure is LinkageError)
        assertTrue(failures.isEmpty())
    }
}
