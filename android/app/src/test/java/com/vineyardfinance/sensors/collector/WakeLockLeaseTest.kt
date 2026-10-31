package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class WakeLockLeaseTest {
    @Test fun renewalsMaintainContinuousLeaseBeyondTwentyMinutes() {
        val clock = FakeClock()
        val lock = FakeTimedWakeLock(clock)
        val lease = WakeLockLease(lock, TEN_MINUTES_MS)

        lease.start()
        assertTrue(lock.isHeld)
        assertEquals(TEN_MINUTES_MS, lock.heldUntilMs)

        clock.advance(NINE_MINUTES_MS)
        assertTrue(lock.isHeld)
        assertTrue(lease.renew())
        assertEquals(19 * MINUTE_MS, lock.heldUntilMs)

        clock.advance(NINE_MINUTES_MS)
        assertTrue(lock.isHeld)
        assertTrue(lease.renew())
        assertEquals(28 * MINUTE_MS, lock.heldUntilMs)

        clock.advance(3 * MINUTE_MS)
        assertEquals(21 * MINUTE_MS, clock.nowMs)
        assertTrue(lock.isHeld)
        assertEquals(3, lock.acquireCount)
        assertEquals(2, lock.releaseCount)

        lease.stop()
        assertFalse(lock.isHeld)
        assertEquals(3, lock.releaseCount)
        assertFalse(lease.renew())
    }

    @Test fun stopIsIdempotentAndStartCannotBeDuplicated() {
        val lock = FakeTimedWakeLock(FakeClock())
        val lease = WakeLockLease(lock, 1_000)
        lease.start()
        assertTrue(runCatching { lease.start() }.isFailure)
        lease.stop()
        lease.stop()
        assertEquals(1, lock.acquireCount)
        assertEquals(1, lock.releaseCount)
    }

    @Test(expected = IllegalArgumentException::class)
    fun timeoutMustBePositive() {
        WakeLockLease(FakeTimedWakeLock(FakeClock()), 0)
    }

    private class FakeClock(var nowMs: Long = 0) {
        fun advance(deltaMs: Long) {
            nowMs += deltaMs
        }
    }

    private class FakeTimedWakeLock(private val clock: FakeClock) : TimedWakeLock {
        var heldUntilMs = Long.MIN_VALUE
        var acquireCount = 0
        var releaseCount = 0

        override val isHeld: Boolean get() = clock.nowMs < heldUntilMs

        override fun acquire(timeoutMs: Long) {
            acquireCount++
            heldUntilMs = clock.nowMs + timeoutMs
        }

        override fun release() {
            check(isHeld) { "released an expired wake lock" }
            releaseCount++
            heldUntilMs = clock.nowMs
        }
    }

    companion object {
        private const val MINUTE_MS = 60_000L
        private const val NINE_MINUTES_MS = 9 * MINUTE_MS
        private const val TEN_MINUTES_MS = 10 * MINUTE_MS
    }
}
