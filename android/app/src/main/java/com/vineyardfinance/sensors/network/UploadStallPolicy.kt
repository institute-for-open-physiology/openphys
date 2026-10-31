package com.vineyardfinance.sensors.network

/**
 * Pure verdict on whether the upload loop has silently stalled. Every legitimate loop state
 * produces a progress signal within ~31 s (capped backoff), so five minutes of silence with
 * records queued means the loop is stuck, whatever the underlying cause. A server outage does
 * not qualify: failing attempts keep [UploadLoopVitals.Snapshot.attemptStartedAtMs] fresh,
 * and restarting the loop would not help.
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
        if (progressAgeMs < STALL_AFTER_MS) return null
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
