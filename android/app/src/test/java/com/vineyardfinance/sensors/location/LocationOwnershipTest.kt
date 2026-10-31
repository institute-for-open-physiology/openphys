package com.vineyardfinance.sensors.location

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class LocationOwnershipTest {
    @Test fun onlyOneOwnerAndCancellationHandsOverAfterCleanup() = runTest {
        val ownership = LocationOwnership()
        val events = mutableListOf<String>()
        val h10 = launch {
            ownership.collect({ true }) {
                events += "h10"
                try { awaitCancellation() } finally { events += "h10 cleanup" }
            }
        }
        runCurrent()
        val seca = launch { ownership.collect({ true }) { events += "seca" } }
        runCurrent()
        assertEquals(listOf("h10"), events)
        h10.cancelAndJoin()
        seca.join()
        assertEquals(listOf("h10", "h10 cleanup", "seca"), events)
    }

    @Test fun stoppedOrConsentRevokedWaiterDoesNotStart() = runTest {
        val ownership = LocationOwnership()
        val release = CompletableDeferred<Unit>()
        var permitted = true
        var starts = 0
        val owner = launch { ownership.collect({ true }) { release.await() } }
        runCurrent()
        val waiter = launch { ownership.collect({ permitted }) { starts++ } }
        runCurrent()
        permitted = false
        release.complete(Unit)
        owner.join(); waiter.join()
        assertEquals(0, starts)
        ownership.collect({ true }) { starts++ }
        assertEquals(1, starts)
    }

    @Test fun cancelledWaiterAndFailedOwnerReleaseLease() = runTest {
        val ownership = LocationOwnership()
        val owner = launch { ownership.collect({ true }) { awaitCancellation() } }
        runCurrent()
        var starts = 0
        val waiter = launch { ownership.collect({ true }) { starts++ } }
        runCurrent()
        waiter.cancelAndJoin(); owner.cancelAndJoin()
        try { ownership.collect({ true }) { throw IllegalStateException("provider failed") } }
        catch (_: IllegalStateException) { }
        ownership.collect({ true }) { starts++ }
        assertEquals(1, starts)
    }
}
