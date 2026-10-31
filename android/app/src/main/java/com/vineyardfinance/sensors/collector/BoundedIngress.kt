package com.vineyardfinance.sensors.collector

import kotlinx.coroutines.channels.Channel
import java.util.UUID

internal enum class EnqueueResult { ENQUEUED, FULL, CLOSED }

/** A deliberately bounded callback-to-coroutine handoff. */
internal class BoundedIngress<T>(val capacity: Int) {
    init {
        require(capacity > 0) { "capacity must be positive" }
    }

    private val channel = Channel<T>(capacity)

    fun tryEnqueue(value: T): EnqueueResult {
        val result = channel.trySend(value)
        return when {
            result.isSuccess -> EnqueueResult.ENQUEUED
            result.isClosed -> EnqueueResult.CLOSED
            else -> EnqueueResult.FULL
        }
    }

    suspend fun receive(): T = channel.receive()

    fun close() {
        channel.close()
    }
}

internal data class RoutedGattNotification(
    val uuid: UUID,
    val value: ByteArray,
    val unixNs: Long,
    val elapsedNs: Long,
)

internal enum class NotificationRouteResult { IGNORED, ENQUEUED, FULL, CLOSED }

/**
 * Only low-rate command responses need temporary buffering. High-rate PMD/HR
 * notifications are persisted directly by the callback consumer and never
 * enter this inbox.
 */
internal class CommandResponseRouter(
    private val responseUuids: Set<UUID>,
    capacity: Int,
) {
    private val inbox = BoundedIngress<RoutedGattNotification>(capacity)

    fun route(notification: RoutedGattNotification): NotificationRouteResult {
        if (notification.uuid !in responseUuids) return NotificationRouteResult.IGNORED
        return when (inbox.tryEnqueue(notification)) {
            EnqueueResult.ENQUEUED -> NotificationRouteResult.ENQUEUED
            EnqueueResult.FULL -> NotificationRouteResult.FULL
            EnqueueResult.CLOSED -> NotificationRouteResult.CLOSED
        }
    }

    suspend fun receiveMatching(uuid: UUID, predicate: (ByteArray) -> Boolean): ByteArray {
        while (true) {
            val next = inbox.receive()
            if (next.uuid == uuid && predicate(next.value)) return next.value
        }
    }

    fun close() = inbox.close()
}
