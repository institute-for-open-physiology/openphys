package com.vineyardfinance.sensors.collector

import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.UUID

class BoundedIngressTest {
    @Test fun sustainedStreamingNotificationsNeverEnterCommandResponseBuffer() = runTest {
        val responseUuid = UUID.randomUUID()
        val streamingUuid = UUID.randomUUID()
        val router = CommandResponseRouter(setOf(responseUuid), capacity = 4)

        repeat(250_000) { sequence ->
            assertEquals(
                NotificationRouteResult.IGNORED,
                router.route(notification(streamingUuid, sequence)),
            )
        }

        repeat(4) { sequence ->
            assertEquals(
                NotificationRouteResult.ENQUEUED,
                router.route(notification(responseUuid, sequence)),
            )
        }
        assertEquals(
            NotificationRouteResult.FULL,
            router.route(notification(responseUuid, 5)),
        )
        assertArrayEquals(byteArrayOf(2), router.receiveMatching(responseUuid) { it[0] == 2.toByte() })
        assertEquals(
            NotificationRouteResult.ENQUEUED,
            router.route(notification(responseUuid, 6)),
        )
        router.close()
        assertEquals(
            NotificationRouteResult.CLOSED,
            router.route(notification(responseUuid, 7)),
        )
    }

    @Test fun callbackIngressIsStrictlyBoundedAndReportsClosure() = runTest {
        val ingress = BoundedIngress<Int>(capacity = 3)
        assertEquals(3, ingress.capacity)
        assertEquals(EnqueueResult.ENQUEUED, ingress.tryEnqueue(1))
        assertEquals(EnqueueResult.ENQUEUED, ingress.tryEnqueue(2))
        assertEquals(EnqueueResult.ENQUEUED, ingress.tryEnqueue(3))
        assertEquals(EnqueueResult.FULL, ingress.tryEnqueue(4))
        assertEquals(1, ingress.receive())
        assertEquals(EnqueueResult.ENQUEUED, ingress.tryEnqueue(4))
        ingress.close()
        assertEquals(EnqueueResult.CLOSED, ingress.tryEnqueue(5))
    }

    @Test fun invalidCapacityIsRejected() {
        val error = runCatching { BoundedIngress<Int>(0) }.exceptionOrNull()
        assertTrue(error is IllegalArgumentException)
        assertFalse(error?.message.isNullOrBlank())
    }

    private fun notification(uuid: UUID, value: Int) = RoutedGattNotification(
        uuid = uuid,
        value = byteArrayOf(value.toByte()),
        unixNs = value.toLong(),
        elapsedNs = value.toLong(),
    )
}
