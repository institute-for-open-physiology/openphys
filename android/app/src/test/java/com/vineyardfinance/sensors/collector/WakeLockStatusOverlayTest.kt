package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class WakeLockStatusOverlayTest {
    @Test
    fun failurePreservesConnectionAndSuccessRestoresOnlyHiddenStatusFields() {
        val overlay = WakeLockStatusOverlay()
        val generation = Any()
        overlay.activate(generation)
        val streaming = CollectorSnapshot(
            state = "Streaming",
            detail = "ECG + ACC + HR",
            connected = true,
            backlogRecords = 4,
        )

        val failed = overlay.renewalFailed(generation, streaming, "lease expired")

        assertEquals("Action needed", failed.state)
        assertEquals(WakeLockStatusOverlay.RENEWAL_FAILURE_DETAIL, failed.detail)
        assertEquals("lease expired", failed.lastError)
        assertEquals(true, failed.connected)

        val withNewTelemetry = failed.copy(
            connected = false,
            backlogRecords = 9,
            rssiDbm = -61,
        )
        val recovered = overlay.renewalSucceeded(generation, withNewTelemetry)

        assertEquals("Streaming", recovered.state)
        assertEquals("ECG + ACC + HR", recovered.detail)
        assertEquals(null, recovered.lastError)
        assertEquals(false, recovered.connected)
        assertEquals(9, recovered.backlogRecords)
        assertEquals(-61, recovered.rssiDbm)
    }

    @Test
    fun repeatedFailureRetainsOriginalStatusAndRestoresItsError() {
        val overlay = WakeLockStatusOverlay()
        val generation = Any()
        overlay.activate(generation)
        val reconnecting = CollectorSnapshot(
            state = "Reconnecting",
            detail = "connect timed out",
            lastError = "connect timed out",
        )
        val first = overlay.renewalFailed(generation, reconnecting, "first renewal failure")
        val second = overlay.renewalFailed(generation, first, "second renewal failure")

        assertEquals("second renewal failure", second.lastError)
        val recovered = overlay.renewalSucceeded(generation, second)
        assertEquals("Reconnecting", recovered.state)
        assertEquals("connect timed out", recovered.detail)
        assertEquals("connect timed out", recovered.lastError)
    }

    @Test
    fun newerCollectorStateWinsOverWakeLockRecovery() {
        val overlay = WakeLockStatusOverlay()
        val generation = Any()
        overlay.activate(generation)
        val failed = overlay.renewalFailed(
            generation,
            CollectorSnapshot(state = "Streaming", detail = "ECG + ACC + HR", connected = true),
            "lease expired",
        )
        val fatal = failed.transitionTo(
            state = "Collection failed",
            detail = "database unavailable",
            error = "database unavailable",
        )

        assertSame(fatal, overlay.renewalFailed(generation, fatal, "another renewal failure"))
        assertSame(fatal, overlay.renewalSucceeded(generation, fatal))
        assertSame(fatal, overlay.renewalSucceeded(generation, fatal))
    }

    @Test
    fun releaseRestoresOwnedStatusAndRejectsLateEvents() {
        val overlay = WakeLockStatusOverlay()
        val generation = Any()
        overlay.activate(generation)
        val connecting = CollectorSnapshot(state = "Connecting", detail = "H10", connected = false)
        val failed = overlay.renewalFailed(
            generation,
            connecting,
            "lease expired",
        )

        val released = overlay.release(generation, failed)

        assertEquals(connecting, released)
        assertSame(released, overlay.renewalFailed(generation, released, "late failure"))
        assertSame(released, overlay.renewalSucceeded(generation, released))
    }

    @Test
    fun staleReleaseCannotInvalidateNewGeneration() {
        val overlay = WakeLockStatusOverlay()
        val oldGeneration = Any()
        val newGeneration = Any()
        overlay.activate(oldGeneration)
        overlay.renewalFailed(
            oldGeneration,
            CollectorSnapshot(state = "Connecting", detail = "H10", connected = false),
            "lease expired",
        )
        overlay.activate(newGeneration)
        val streaming = CollectorSnapshot(state = "Streaming", detail = "ECG + ACC + HR", connected = true)
        val failed = overlay.renewalFailed(newGeneration, streaming, "new lease failure")

        assertSame(failed, overlay.release(oldGeneration, failed))
        assertEquals(streaming, overlay.renewalSucceeded(newGeneration, failed))
    }

    @Test
    fun releaseAndStoppedStateWinOverAlreadyInFlightLateFailure() {
        val overlay = WakeLockStatusOverlay()
        val lease = WakeLockLease(FakeTimedWakeLock(), 1_000)
        lease.start()
        overlay.activate(lease)
        val store = SerializedCollectorStatus(
            CollectorSnapshot(state = "Streaming", detail = "ECG + ACC + HR", connected = true),
        )
        val renewalEntered = CountDownLatch(1)
        val finishRenewal = CountDownLatch(1)
        val renewalDone = CountDownLatch(1)
        val renewalResult = AtomicReference<Boolean>()
        val renewalThread = Thread {
            renewalEntered.countDown()
            check(finishRenewal.await(2, TimeUnit.SECONDS))
            val renewed = lease.renew()
            renewalResult.set(renewed)
            if (!renewed) {
                store.update { overlay.renewalFailed(lease, it, "lease stopped during renewal") }
            }
            renewalDone.countDown()
        }
        renewalThread.start()
        check(renewalEntered.await(2, TimeUnit.SECONDS))

        store.update { overlay.release(lease, it) }
        lease.stop()
        store.update { it.transitionTo("Stopped", "Collection disabled") }
        finishRenewal.countDown()

        check(renewalDone.await(2, TimeUnit.SECONDS))
        renewalThread.join(2_000)
        assertFalse(renewalResult.get())
        assertEquals(
            CollectorSnapshot(state = "Stopped", detail = "Collection disabled"),
            store.updates.value,
        )
    }

    @Test
    fun eventsBeforeActivationAreIgnored() {
        val overlay = WakeLockStatusOverlay()
        val generation = Any()
        val streaming = CollectorSnapshot(state = "Streaming", detail = "ECG + ACC + HR", connected = true)

        assertSame(streaming, overlay.renewalFailed(generation, streaming, "failure"))
        assertSame(streaming, overlay.renewalSucceeded(generation, streaming))
        assertSame(streaming, overlay.release(generation, streaming))
    }

    private class FakeTimedWakeLock : TimedWakeLock {
        private var held = false
        override val isHeld: Boolean get() = held

        override fun acquire(timeoutMs: Long) {
            held = true
        }

        override fun release() {
            held = false
        }
    }
}
