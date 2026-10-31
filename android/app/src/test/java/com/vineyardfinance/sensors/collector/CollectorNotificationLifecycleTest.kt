package com.vineyardfinance.sensors.collector

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class CollectorNotificationLifecycleTest {
    @Test
    fun preRunFatalAndCleanupStatusesAreDeliveredSynchronouslyBeforeClose() {
        val store = SerializedCollectorStatus()
        val notified = mutableListOf<NotificationContent>()
        val queued = mutableListOf<() -> Unit>()
        val sink = LifecycleNotificationSink(
            isDispatchThread = { true },
            dispatch = queued::add,
            onNotify = notified::add,
        )
        val owner = NotificationOwner(onNotify = sink::offer, clock = { 0L })
        owner.markCurrentContentPosted()
        val bridge = StatusNotificationBridge(store::observeNotifications, CollectorNotificationMapper(owner) { 1_000L })
        bridge.onCreate()

        store.update { it.copy(state = "Action needed", detail = "Grant Bluetooth permissions") }
        store.update { it.copy(state = "Collection failed", detail = "fatal", lastError = "fatal") }
        store.update { it.copy(detail = "Service cleanup record failed", lastError = "disk full") }
        assertEquals(0, notified.size)
        assertEquals(1, queued.size)

        // Models CollectorService's main-dispatch barrier at the end of its
        // NonCancellable finally block, before stopSelf/onDestroy closes either
        // lifecycle owner.
        queued.toList().forEach { it() }
        queued.clear()
        assertEquals("H10: Action needed", notified[0].title)
        assertEquals("⚠ fatal · queue status checking", notified[1].text)
        assertTrue(notified.last().text.startsWith("⚠ disk full · Service cleanup record failed"))
        assertEquals(3, notified.size)

        bridge.close()
        sink.close()
        store.update { it.copy(state = "Stopped", detail = "Collection disabled", lastError = null) }
        assertEquals(3, notified.size)
        assertEquals(0, queued.size)
    }

    @Test
    fun atomicMapperReadsClockOnceAndMakesAtMostOneDecision() {
        val notified = mutableListOf<NotificationContent>()
        var wallClockReads = 0
        val owner = NotificationOwner(onNotify = notified::add, clock = { 0L })
        val mapper = CollectorNotificationMapper(owner) {
            wallClockReads += 1
            5_000L
        }

        mapper.onSnapshot(
            CollectorSnapshot(
                state = "Streaming",
                detail = "ECG + ACC + HR",
                connected = true,
                ecgUnixNs = 4_900_000_000L,
                accUnixNs = 5_100_000_000L,
                hrUnixNs = 4_000_000_000L,
                backlogRecords = 7,
                backlogBytes = 700,
                backlogKnown = true,
                batteryPercent = 70,
                rssiDbm = -52,
                lastError = "brief timeout",
            ),
        )

        assertEquals(1, wallClockReads)
        assertEquals(1, notified.size)
        val text = notified.single().text
        assertTrue(text.contains("ECG 100ms"))
        assertTrue(text.contains("ACC 0ms"))
        assertTrue(text.contains("HR 1s"))
        assertTrue(text.contains("queued 7 records"))
    }

    @Test
    fun mapperHandlesMissingTimestampsWithoutFreshnessText() {
        val notified = mutableListOf<NotificationContent>()
        val mapper = CollectorNotificationMapper(NotificationOwner(onNotify = notified::add))

        mapper.onSnapshot(CollectorSnapshot(state = "Scanning", detail = "Polar H10"))

        assertEquals(1, notified.size)
        assertTrue(notified.single().text.contains("queue status checking"))
        assertFalse(notified.single().text.contains("ECG"))
        assertFalse(notified.single().text.contains("ACC"))
        assertFalse(notified.single().text.contains("HR"))
    }

    @Test
    fun oldRegistrationCloseCannotDetachReplacementObserver() {
        val store = SerializedCollectorStatus()
        val first = mutableListOf<CollectorSnapshot>()
        val second = mutableListOf<CollectorSnapshot>()
        val firstRegistration = store.observeNotifications(first::add)
        store.update { it.copy(state = "Scanning") }
        val secondRegistration = store.observeNotifications(second::add)

        firstRegistration.close()
        store.update { it.copy(state = "Streaming") }

        assertEquals(listOf("Stopped", "Scanning"), first.map { it.state })
        assertEquals(listOf("Scanning", "Streaming"), second.map { it.state })
        secondRegistration.close()
        secondRegistration.close()
        store.update { it.copy(state = "Stopped") }
        assertEquals(2, second.size)
        assertEquals("Stopped", store.updates.value.state)
    }

    @Test
    fun statusObserverCanReenterWithoutDeadlockAndRemainsOrdered() {
        val store = SerializedCollectorStatus()
        val states = mutableListOf<String>()
        val registration = store.observeNotifications { snapshot ->
            states += snapshot.state
            if (snapshot.state == "Scanning") {
                store.update { it.copy(state = "Streaming") }
            }
        }

        store.update { it.copy(state = "Scanning") }

        assertEquals(listOf("Stopped", "Scanning", "Streaming"), states)
        registration.close()
    }

    @Test
    fun lifecycleSinkKeepsBinderWorkOffProducerAndPreservesOrder() {
        val queued = mutableListOf<() -> Unit>()
        val notified = mutableListOf<String>()
        val sink = LifecycleNotificationSink(
            isDispatchThread = { true },
            dispatch = queued::add,
            onNotify = { notified += it.title },
        )

        sink.offer(NotificationContent("first", "one", false))
        sink.offer(NotificationContent("second", "two", false))

        // A producer returns after enqueueing; binder-backed work has not run.
        assertEquals(0, notified.size)
        assertEquals(1, queued.size)
        queued.forEach { it() }
        assertEquals(listOf("first", "second"), notified)
    }

    @Test
    fun lifecycleSinkDropsQueuedAndLaterWorkAfterDestroy() {
        val queued = mutableListOf<() -> Unit>()
        val notified = mutableListOf<NotificationContent>()
        val sink = LifecycleNotificationSink(
            isDispatchThread = { true },
            dispatch = queued::add,
            onNotify = notified::add,
        )
        sink.offer(NotificationContent("queued", "before destroy", false))

        sink.close()
        sink.offer(NotificationContent("late", "after destroy", false))
        queued.forEach { it() }

        assertEquals(0, notified.size)
        assertEquals(1, queued.size)
    }

    @Test
    fun lifecycleSinkRequiresExplicitDrainEvenOnLifecycleThread() {
        val queued = mutableListOf<() -> Unit>()
        val notified = mutableListOf<NotificationContent>()
        val sink = LifecycleNotificationSink(
            isDispatchThread = { true },
            dispatch = queued::add,
            onNotify = notified::add,
        )

        sink.offer(NotificationContent("now", "lifecycle thread", false))
        assertEquals(0, notified.size)
        sink.drainOnLifecycleThread()

        assertEquals("now", notified.single().title)
    }

    @Test
    fun platformCallbackCanUpdateStatusWithoutWaitingOnStatusMonitor() {
        val store = SerializedCollectorStatus()
        val queued = mutableListOf<() -> Unit>()
        val executor = Executors.newSingleThreadExecutor()
        lateinit var sink: LifecycleNotificationSink
        val owner = NotificationOwner(
            onNotify = { sink.offer(it) },
            clock = { 0L },
        )
        owner.markCurrentContentPosted()
        sink = LifecycleNotificationSink(
            isDispatchThread = { true },
            dispatch = queued::add,
            onNotify = { content ->
                if (content.title == "H10: Scanning") {
                    executor.submit {
                        store.update { it.transitionTo("Streaming", "ECG + ACC + HR", connected = true) }
                    }.get(2, TimeUnit.SECONDS)
                }
            },
        )
        val bridge = StatusNotificationBridge(store::observeNotifications, CollectorNotificationMapper(owner) { 0L })
        try {
            bridge.onCreate()
            store.update { it.transitionTo("Scanning", "Polar H10") }

            // If platform delivery still ran under the status monitor, the
            // worker update above would time out waiting for that same monitor.
            sink.drainOnLifecycleThread()
            assertEquals("Streaming", store.updates.value.state)
        } finally {
            bridge.close()
            sink.close()
            executor.shutdownNow()
        }
    }

    @Test
    fun drainRejectsNonLifecycleThread() {
        val sink = LifecycleNotificationSink(
            isDispatchThread = { false },
            dispatch = {},
            onNotify = {},
        )

        assertThrows(IllegalStateException::class.java) { sink.drainOnLifecycleThread() }
    }

    @Test
    fun progressTransitionClearsStaleErrorAndNewFailureCanLatch() {
        val notified = mutableListOf<NotificationContent>()
        val mapper = CollectorNotificationMapper(NotificationOwner(onNotify = notified::add)) { 0L }
        var snapshot = CollectorSnapshot(
            state = "Reconnecting",
            detail = "old timeout",
            lastError = "old timeout",
        )
        mapper.onSnapshot(snapshot)
        assertTrue(notified.last().text.contains("old timeout"))

        snapshot = snapshot.transitionTo("Scanning", "Polar H10")
        mapper.onSnapshot(snapshot)
        assertFalse(notified.last().text.contains("old timeout"))

        snapshot = snapshot.transitionTo(
            state = "Action needed",
            detail = "Wake lock renewal failed; retrying",
            error = "new wake-lock error",
        )
        mapper.onSnapshot(snapshot)
        assertTrue(notified.last().text.contains("new wake-lock error"))
    }

    @Test
    fun bridgeAllowsOneActiveRegistrationAndCloseIsIdempotent() {
        var subscriptions = 0
        var closes = 0
        val bridge = StatusNotificationBridge(
            subscribe = {
                subscriptions += 1
                AutoCloseable { closes += 1 }
            },
            mapper = CollectorNotificationMapper(NotificationOwner(onNotify = {})),
        )

        bridge.onCreate()
        assertThrows(IllegalStateException::class.java) { bridge.onCreate() }
        bridge.close()
        bridge.close()

        assertEquals(1, subscriptions)
        assertEquals(1, closes)
    }
}
