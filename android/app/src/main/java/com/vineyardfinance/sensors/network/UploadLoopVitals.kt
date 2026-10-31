package com.vineyardfinance.sensors.network

/**
 * Thread-safe liveness facts about the upload loop, updated by [IngestUploader] and read by
 * the collector_health emitter and the stall watchdog. Timestamps are readings of the
 * injected clock; zero means the event has never happened. Production injects
 * SystemClock.elapsedRealtime (monotonic, counts through Doze) so device clock steps can
 * neither fake nor mask a stall — every reader must use the same clock base.
 */
class UploadLoopVitals(private val clock: () -> Long = System::currentTimeMillis) {
    private val lock = Any()
    private var loopLaunchedAtMs = 0L
    private var iterationAtMs = 0L
    private var attemptStartedAtMs = 0L
    private var outcomeAtMs = 0L
    private var lastAckAtMs = 0L
    private var lastOutcome: String? = null
    private var backoffMs = 0L
    private var waitingForReconnect = false
    private var consecutiveFailures = 0

    fun loopLaunched(): Unit = synchronized(lock) {
        loopLaunchedAtMs = clock()
        // A loop cancelled mid reconnect-wait must not leave a stale flag that would blind
        // the stall watchdog for the relaunched loop.
        waitingForReconnect = false
    }

    fun iteration(): Unit = synchronized(lock) { iterationAtMs = clock() }

    fun attemptStarted(): Unit = synchronized(lock) { attemptStartedAtMs = clock() }

    fun ackReceived(): Unit = synchronized(lock) {
        val now = clock()
        outcomeAtMs = now
        lastAckAtMs = now
        lastOutcome = "durable ack"
        consecutiveFailures = 0
    }

    fun attemptFailed(message: String): Unit = synchronized(lock) {
        outcomeAtMs = clock()
        lastOutcome = message.take(160)
        consecutiveFailures += 1
    }

    fun backoff(delayMs: Long): Unit = synchronized(lock) { backoffMs = delayMs }

    fun reconnectWait(waiting: Boolean): Unit = synchronized(lock) { waitingForReconnect = waiting }

    fun snapshot(): Snapshot = synchronized(lock) {
        Snapshot(
            loopLaunchedAtMs = loopLaunchedAtMs,
            iterationAtMs = iterationAtMs,
            attemptStartedAtMs = attemptStartedAtMs,
            outcomeAtMs = outcomeAtMs,
            lastAckAtMs = lastAckAtMs,
            lastOutcome = lastOutcome,
            backoffMs = backoffMs,
            waitingForReconnect = waitingForReconnect,
            consecutiveFailures = consecutiveFailures,
        )
    }

    data class Snapshot(
        val loopLaunchedAtMs: Long,
        val iterationAtMs: Long,
        val attemptStartedAtMs: Long,
        val outcomeAtMs: Long,
        val lastAckAtMs: Long,
        val lastOutcome: String?,
        val backoffMs: Long,
        val waitingForReconnect: Boolean,
        val consecutiveFailures: Int,
    )

    companion object {
        /** Age of a vitals timestamp, or -1 when the event has never happened. */
        fun ageMs(nowMs: Long, atMs: Long): Long =
            if (atMs <= 0L) -1L else (nowMs - atMs).coerceAtLeast(0L)
    }
}
