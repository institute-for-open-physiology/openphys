package com.vineyardfinance.sensors.collector

/**
 * Owns the temporary user-visible status for a wake-lock renewal incident.
 *
 * Renewal does not change BLE connectivity. Recovery restores only the status
 * fields hidden by this exact incident; live telemetry and the current
 * connected bit remain authoritative. Any intervening status transition wins.
 */
internal class WakeLockStatusOverlay {
    private data class HiddenStatus(
        val state: String,
        val detail: String,
        val lastError: String?,
    )

    private var hiddenStatus: HiddenStatus? = null
    private var ownedError: String? = null
    private var activeGeneration: Any? = null

    @Synchronized
    fun activate(generation: Any) {
        activeGeneration = generation
        clearIncident()
    }

    @Synchronized
    fun renewalFailed(generation: Any, snapshot: CollectorSnapshot, error: String): CollectorSnapshot {
        if (activeGeneration !== generation) return snapshot
        if (hiddenStatus != null && !owns(snapshot)) return snapshot
        if (hiddenStatus == null) {
            hiddenStatus = HiddenStatus(snapshot.state, snapshot.detail, snapshot.lastError)
        }
        ownedError = error
        return snapshot.copy(
            state = ACTION_NEEDED_STATE,
            detail = RENEWAL_FAILURE_DETAIL,
            lastError = error,
        )
    }

    @Synchronized
    fun renewalSucceeded(generation: Any, snapshot: CollectorSnapshot): CollectorSnapshot {
        if (activeGeneration !== generation) return snapshot
        return restoreOwnedStatus(snapshot)
    }

    /** Revokes [generation] before the platform lease is stopped. */
    @Synchronized
    fun release(generation: Any, snapshot: CollectorSnapshot): CollectorSnapshot {
        if (activeGeneration !== generation) return snapshot
        activeGeneration = null
        return restoreOwnedStatus(snapshot)
    }

    private fun restoreOwnedStatus(snapshot: CollectorSnapshot): CollectorSnapshot {
        val hidden = hiddenStatus?.takeIf { owns(snapshot) }
        clearIncident()
        return if (hidden != null) {
            snapshot.copy(
                state = hidden.state,
                detail = hidden.detail,
                lastError = hidden.lastError,
            )
        } else {
            snapshot
        }
    }

    private fun clearIncident() {
        hiddenStatus = null
        ownedError = null
    }

    private fun owns(snapshot: CollectorSnapshot): Boolean =
        hiddenStatus != null &&
            snapshot.state == ACTION_NEEDED_STATE &&
            snapshot.detail == RENEWAL_FAILURE_DETAIL &&
            snapshot.lastError == ownedError

    companion object {
        const val RENEWAL_FAILURE_DETAIL = "Wake lock renewal failed; retrying"
        private const val ACTION_NEEDED_STATE = "Action needed"
    }
}
