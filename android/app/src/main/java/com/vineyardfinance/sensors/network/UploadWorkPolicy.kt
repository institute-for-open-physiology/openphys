package com.vineyardfinance.sensors.network

internal enum class UploadWorkOutcome { SUCCESS, RETRY, FAILURE }

/**
 * A residual worker is the final durable owner when the foreground service is stopped. Transient
 * failures therefore retry without an attempt ceiling; only absent/revoked auth is terminal until
 * the explicit post-enrollment enqueue creates a fresh worker.
 */
internal object UploadWorkPolicy {
    fun decide(
        credentialAvailable: Boolean,
        drain: IngestUploader.DrainResult?,
        runAttemptCount: Int,
    ): UploadWorkOutcome {
        require(runAttemptCount >= 0) { "worker attempt count must not be negative" }
        if (!credentialAvailable) return UploadWorkOutcome.FAILURE
        val result = requireNotNull(drain) { "credentialed worker requires a drain result" }
        return when {
            result.complete -> UploadWorkOutcome.SUCCESS
            result.reconnectRequired -> UploadWorkOutcome.FAILURE
            else -> UploadWorkOutcome.RETRY
        }
    }
}
