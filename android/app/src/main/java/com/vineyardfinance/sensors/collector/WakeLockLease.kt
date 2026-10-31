package com.vineyardfinance.sensors.collector

internal interface TimedWakeLock {
    val isHeld: Boolean
    fun acquire(timeoutMs: Long)
    fun release()
}

/** Maintains one renewable, non-reference-counted wake-lock lease. */
internal class WakeLockLease(
    private val wakeLock: TimedWakeLock,
    private val timeoutMs: Long,
) {
    init {
        require(timeoutMs > 0) { "wake-lock timeout must be positive" }
    }

    private var active = false

    @Synchronized
    fun start() {
        check(!active) { "wake-lock lease is already active" }
        wakeLock.acquire(timeoutMs)
        active = true
    }

    /** Releases the old timed lease and immediately starts a full new lease. */
    @Synchronized
    fun renew(): Boolean {
        if (!active) return false
        if (wakeLock.isHeld) wakeLock.release()
        wakeLock.acquire(timeoutMs)
        return true
    }

    @Synchronized
    fun stop() {
        if (!active) return
        if (wakeLock.isHeld) wakeLock.release()
        active = false
    }
}
