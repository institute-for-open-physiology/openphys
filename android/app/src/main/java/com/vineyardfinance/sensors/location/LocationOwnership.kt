package com.vineyardfinance.sensors.location

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** One phone location listener set, shared by independently running ECG services. */
class LocationOwnership {
    private val mutex = Mutex()

    suspend fun collect(active: () -> Boolean, block: suspend () -> Unit) {
        // The owning service's cancellation bounds this wait. Recheck after the
        // handover: a waiting service may have stopped or lost consent meanwhile.
        mutex.withLock { if (active()) block() }
    }
}
