package com.vineyardfinance.sensors.network

/**
 * Pure verdict on whether the upload loop has silently stalled. Every legitimate loop state
 * produces a progress signal within its current backoff, so five minutes of silence beyond
 * that backoff with records queued means the loop is stuck, whatever the underlying cause.
 * A server outage does not qualify: the loop is sleeping its recorded backoff, and restarting
 * it would only reset the backoff.
 */
object UploadStallPolicy {
    const val STALL_AFTER_MS = 5 * 60_000L

    data class Report(
        val queuedRecords: Long,
        val progressAgeMs: Long,
        val attemptAgeMs: Long,
        val ackAgeMs: Long,
        val backoffMs: Long,
        val consecutiveFailures: Int,
        val lastOutcome: String?,
    )

    /** Returns a stall report, or null while the loop is behaving. */
    fun evaluate(nowMs: Long, vitals: UploadLoopVitals.Snapshot, queuedRecords: Long): Report? {
        // queuedRecords < 0 means the backlog could not be read; stay blind rather than
        // trigger-happy — queue-read failures are surfaced loudly through the status path.
        if (queuedRecords <= 0) return null
        // Waiting for the user to re-enroll the phone is user-actionable and already
        // surfaced; restarting the loop cannot help.
        if (vitals.waitingForReconnect) return null
        val lastProgressMs = maxOf(vitals.loopLaunchedAtMs, vitals.attemptStartedAtMs, vitals.outcomeAtMs)
        if (lastProgressMs <= 0) return null
        val progressAgeMs = nowMs - lastProgressMs
        // After a failed attempt the loop sleeps its backoff before the next attempt starts.
        val sleepingMs = if (vitals.outcomeAtMs >= vitals.attemptStartedAtMs) vitals.backoffMs else 0L
        if (progressAgeMs < STALL_AFTER_MS + sleepingMs) return null
        return Report(
            queuedRecords = queuedRecords,
            progressAgeMs = progressAgeMs,
            attemptAgeMs = UploadLoopVitals.ageMs(nowMs, vitals.attemptStartedAtMs),
            ackAgeMs = UploadLoopVitals.ageMs(nowMs, vitals.lastAckAtMs),
            backoffMs = vitals.backoffMs,
            consecutiveFailures = vitals.consecutiveFailures,
            lastOutcome = vitals.lastOutcome,
        )
    }
}
