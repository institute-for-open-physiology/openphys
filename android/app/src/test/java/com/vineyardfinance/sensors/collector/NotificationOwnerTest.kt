package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

class NotificationOwnerTest {
    private val notified = mutableListOf<NotificationContent>()
    private var clockMs = 0L
    private lateinit var owner: NotificationOwner
    private var snapshot = NotificationOwner.Snapshot()

    @Before
    fun setUp() {
        notified.clear()
        clockMs = 0L
        snapshot = NotificationOwner.Snapshot(backlogKnown = true)
        owner = NotificationOwner(
            onNotify = notified::add,
            telemetryMinIntervalMs = 2_000L,
            clock = { clockMs },
            initialSnapshot = snapshot,
        )
    }

    private fun update(block: (NotificationOwner.Snapshot) -> NotificationOwner.Snapshot) {
        snapshot = block(snapshot)
        owner.onSnapshot(snapshot)
    }

    @Test
    fun initialContentReflectsStoppedStateWithoutNotify() {
        val content = owner.content()
        assertEquals("H10: Stopped", content.title)
        assertTrue(content.text.contains("Collection is disabled"))
        assertFalse(content.showPairingAction)
        assertEquals(0, notified.size)
    }

    @Test
    fun foregroundPostSuppressesIdenticalInitialSnapshot() {
        owner.markCurrentContentPosted()
        owner.onSnapshot(snapshot)
        assertEquals(0, notified.size)
    }

    @Test
    fun unknownAndUnavailableBacklogNeverRenderAsMeasuredZero() {
        val updates = mutableListOf<NotificationContent>()
        val unknown = NotificationOwner(onNotify = updates::add)
        assertTrue(unknown.content().text.contains("queue status checking"))
        assertFalse(unknown.content().text.contains("queued 0 records"))

        unknown.markCurrentContentPosted()
        unknown.onSnapshot(
            NotificationOwner.Snapshot(
                backlogStatusError = "Room observer failed",
            ),
        )

        assertEquals(1, updates.size)
        assertTrue(updates.single().text.contains("queue status unavailable: Room observer failed"))
    }

    @Test
    fun oneAtomicSnapshotMakesOneNotification() {
        update {
            it.copy(
                state = "Collection failed",
                detail = "Service cleanup record failed",
                lastError = "disk full",
                backlogRecords = 42,
                batteryPercent = 87,
                rssiDbm = -51,
                ecgAgeMs = 100,
                accAgeMs = 200,
                hrAgeMs = 300,
            )
        }

        assertEquals(1, notified.size)
        val content = notified.single()
        assertEquals("H10: Collection failed", content.title)
        assertTrue(content.text.contains("⚠ disk full · Service cleanup record failed"))
        assertTrue(content.text.contains("queued 42 records"))
        assertTrue(content.text.contains("battery 87%"))
        assertTrue(content.text.contains("RSSI -51dBm"))
        assertTrue(content.text.contains("ECG 100ms ACC 200ms HR 300ms"))
    }

    @Test
    fun equalErrorAndDetailRenderedOnlyOnce() {
        update { it.copy(state = "Collection failed", detail = "disk full", lastError = "disk full") }

        val text = notified.single().text
        assertEquals(1, Regex("disk full").findAll(text).count())
        assertTrue(text.startsWith("⚠ disk full · queued"))
    }

    @Test
    fun clearingErrorIsImmediateWithoutStateOrDetailChange() {
        update { it.copy(state = "Reconnecting", detail = "retrying", lastError = "timeout") }
        assertTrue(notified.last().text.contains("⚠ timeout"))

        update { it.copy(lastError = null) }

        assertEquals(2, notified.size)
        assertFalse(notified.last().text.contains("⚠"))
        assertFalse(notified.last().text.contains("timeout"))
        assertTrue(notified.last().text.startsWith("retrying"))
    }

    @Test
    fun stateDetailAndFailureTransitionsAreImmediate() {
        update { it.copy(state = "Scanning", detail = "Polar H10") }
        update { it.copy(state = "Connecting", detail = "first-address") }
        update { it.copy(detail = "second-address") }
        update { it.copy(lastError = "watchdog timeout") }

        assertEquals(4, notified.size)
        assertEquals("H10: Scanning", notified[0].title)
        assertEquals("H10: Connecting", notified[1].title)
        assertTrue(notified[2].text.startsWith("second-address"))
        assertTrue(notified[3].text.startsWith("⚠ watchdog timeout"))
    }

    @Test
    fun identicalSnapshotDoesNotNotifyTwice() {
        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        owner.onSnapshot(snapshot)
        owner.onSnapshot(snapshot)
        assertEquals(1, notified.size)
    }

    @Test
    fun pairingActionOnlyShownForPairingState() {
        update { it.copy(state = "Pairing required", detail = "Confirm pairing prompt") }
        assertTrue(notified.last().showPairingAction)

        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        assertFalse(notified.last().showPairingAction)
    }

    @Test
    fun highRateFreshnessIsCoalescedAtCadence() {
        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        update { it.copy(backlogRecords = 1, connected = true) }
        notified.clear()

        for (ms in 100L..6_000L step 100L) {
            clockMs = ms
            update { it.copy(ecgAgeMs = ms, accAgeMs = ms, hrAgeMs = ms) }
        }

        assertEquals(3, notified.size)
        assertTrue(notified.last().text.contains("ECG 6s ACC 6s HR 6s"))
    }

    @Test
    fun telemetryIsDeferredUntilIntervalThenLatestValuePublishes() {
        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        update { it.copy(backlogRecords = 1) }
        notified.clear()

        clockMs = 500L
        update { it.copy(backlogRecords = 2, ecgAgeMs = 500) }
        assertEquals(0, notified.size)

        clockMs = 2_000L
        owner.onSnapshot(snapshot)
        assertEquals(1, notified.size)
        assertTrue(notified.single().text.contains("queued 2 records"))
        assertTrue(notified.single().text.contains("ECG 500ms"))
    }

    @Test
    fun telemetryOnlyPublishesWhenRenderedContentChanges() {
        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        notified.clear()

        update { it.copy(connected = true) }
        clockMs = 3_000L
        owner.onSnapshot(snapshot)
        assertEquals(0, notified.size)

        update { it.copy(backlogRecords = 1) }
        assertEquals(1, notified.size)
    }

    @Test
    fun telemetryRateIsBoundedOverTenSeconds() {
        update { it.copy(state = "Streaming", detail = "ECG + ACC + HR") }
        notified.clear()

        for (ms in 0L..10_200L step 100L) {
            clockMs = ms
            update { it.copy(backlogRecords = ms / 100L) }
        }

        assertTrue("telemetry notified ${notified.size} times", notified.size in 1..6)
        assertTrue(notified.last().text.contains("queued 101 records"))
    }

    @Test
    fun freshnessUsesMillisecondAndSecondLabels() {
        update {
            it.copy(
                state = "Streaming",
                detail = "ECG + ACC + HR",
                ecgAgeMs = 999,
                accAgeMs = 1_000,
                hrAgeMs = 1_500,
            )
        }

        val text = notified.single().text
        assertTrue(text.contains("ECG 999ms"))
        assertTrue(text.contains("ACC 1s"))
        assertTrue(text.contains("HR 1s"))
    }

    @Test
    fun defaultClockIsMonotonicAndAllowsFirstTelemetry() {
        val first = NotificationOwner.monotonicClockMs()
        val second = NotificationOwner.monotonicClockMs()
        assertTrue("monotonic clock went backwards: $first -> $second", second >= first)

        val defaultNotifications = mutableListOf<NotificationContent>()
        val defaultOwner = NotificationOwner(onNotify = defaultNotifications::add)
        defaultOwner.onSnapshot(NotificationOwner.Snapshot(backlogRecords = 1, backlogKnown = true))
        assertEquals(1, defaultNotifications.size)
    }

    @Test
    fun negativeTelemetryIntervalIsRejected() {
        assertThrows(IllegalArgumentException::class.java) {
            NotificationOwner(onNotify = {}, telemetryMinIntervalMs = -1)
        }
    }
}
