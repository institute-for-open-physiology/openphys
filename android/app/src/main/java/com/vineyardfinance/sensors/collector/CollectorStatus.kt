package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.data.BacklogStats
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

data class CollectorSnapshot(
    val state: String = "Stopped",
    val detail: String = "Collection is disabled",
    val serviceRunning: Boolean = false,
    val connected: Boolean = false,
    val ecgUnixNs: Long? = null,
    val accUnixNs: Long? = null,
    val hrUnixNs: Long? = null,
    val rssiDbm: Int? = null,
    val batteryPercent: Int? = null,
    val backlogRecords: Long = 0,
    val backlogKnown: Boolean = false,
    val backlogStatusError: String? = null,
    val lastAck: String? = null,
    val lastAckUnixMs: Long? = null,
    val lastError: String? = null,
)

/**
 * Serializes status mutation and the foreground-notification observer.
 *
 * The StateFlow remains the public UI feed. The single notification observer
 * is deliberately synchronous: when [update] returns, the service-lifetime
 * observer has already seen that exact snapshot. A generation token prevents
 * a late close from an old service instance from detaching a newer observer.
 */
internal class SerializedCollectorStatus(
    initial: CollectorSnapshot = CollectorSnapshot(),
) {
    private data class Observer(
        val generation: Any,
        val callback: (CollectorSnapshot) -> Unit,
    )

    private val lock = Any()
    private val mutable = MutableStateFlow(initial)
    val updates: StateFlow<CollectorSnapshot> = mutable.asStateFlow()
    private var notificationObserver: Observer? = null

    fun update(block: (CollectorSnapshot) -> CollectorSnapshot) {
        synchronized(lock) {
            val next = block(mutable.value)
            mutable.value = next
            notificationObserver?.callback?.invoke(next)
        }
    }

    fun observeNotifications(callback: (CollectorSnapshot) -> Unit): AutoCloseable {
        val generation = Any()
        synchronized(lock) {
            notificationObserver = Observer(generation, callback)
            callback(mutable.value)
        }
        return AutoCloseable {
            synchronized(lock) {
                if (notificationObserver?.generation === generation) {
                    notificationObserver = null
                }
            }
        }
    }
}

object CollectorStatus {
    private val store = SerializedCollectorStatus()
    val updates: StateFlow<CollectorSnapshot> = store.updates

    fun update(block: (CollectorSnapshot) -> CollectorSnapshot) = store.update(block)

    fun observeNotifications(callback: (CollectorSnapshot) -> Unit): AutoCloseable =
        store.observeNotifications(callback)
}

internal fun CollectorSnapshot.withBacklog(backlog: BacklogStats): CollectorSnapshot = copy(
    backlogRecords = backlog.records,
    backlogKnown = true,
    backlogStatusError = null,
)

internal fun CollectorSnapshot.withBacklogUnavailable(detail: String): CollectorSnapshot = copy(
    backlogKnown = false,
    backlogStatusError = detail.take(240),
)

internal fun CollectorSnapshot.backlogDescription(): String = when {
    backlogKnown -> "$backlogRecords records"
    backlogStatusError != null -> "queue status unavailable: $backlogStatusError"
    else -> "queue status checking"
}
