package com.vineyardfinance.sensors.auth

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class EnrollmentSuccessNoticeTest {
    private var now = 10_000L

    @Test fun ordinaryLaunchIsHiddenEvenWhenConnectionStatusExistsElsewhere() {
        val notice = notice()

        val presentation = notice.present(EnrollmentState.Idle)

        assertFalse(presentation.visible)
        assertFalse(presentation.focusAndAnnounce)
        assertNull(presentation.successToAcknowledge)
        assertNull(presentation.remainingMillis)
        assertNull(notice.savedDeadline())
    }

    @Test fun genuineSuccessIsVisibleFocusedAnnouncedAndAcknowledgedWithoutIdentifiers() {
        val notice = notice()
        val success = EnrollmentState.Succeeded()

        val presentation = notice.present(success)

        assertTrue(presentation.visible)
        assertTrue(presentation.focusAndAnnounce)
        assertSame(success, presentation.successToAcknowledge)
        assertEquals(120_000L, presentation.remainingMillis)
        assertEquals("Succeeded", success.toString())
    }

    @Test fun acknowledgedSuccessStaysVisibleWithoutRepeatedAttention() {
        val notice = notice()
        val success = EnrollmentState.Succeeded()
        notice.present(success)

        now += 1_000L
        val acknowledged = notice.present(EnrollmentState.Idle)

        assertTrue(acknowledged.visible)
        assertFalse(acknowledged.focusAndAnnounce)
        assertNull(acknowledged.successToAcknowledge)
        assertEquals(119_000L, acknowledged.remainingMillis)
    }

    @Test fun theSameSuccessEventCannotRestartOrReannounceAfterExpiry() {
        val notice = notice()
        val success = EnrollmentState.Succeeded()
        notice.present(success)
        now += 1_000L

        val repeatedWhileVisible = notice.present(success)

        assertTrue(repeatedWhileVisible.visible)
        assertFalse(repeatedWhileVisible.focusAndAnnounce)
        assertSame(success, repeatedWhileVisible.successToAcknowledge)
        assertEquals(119_000L, repeatedWhileVisible.remainingMillis)

        now += 119_000L

        val replay = notice.present(success)

        assertFalse(replay.visible)
        assertFalse(replay.focusAndAnnounce)
        assertSame(success, replay.successToAcknowledge)
        assertNull(notice.savedDeadline())
    }

    @Test fun recreationRetainsBoundedFeedbackWithoutReannouncingOrExtendingIt() {
        val original = notice()
        original.present(EnrollmentState.Succeeded())
        now += 30_000L
        val savedDeadline = original.savedDeadline()
        val recreated = notice()

        recreated.restore(savedDeadline)
        val restored = recreated.current()

        assertTrue(restored.visible)
        assertFalse(restored.focusAndAnnounce)
        assertEquals(90_000L, restored.remainingMillis)
        assertEquals(savedDeadline, recreated.savedDeadline())

        now += 90_000L
        assertFalse(recreated.current().visible)
        assertNull(recreated.savedDeadline())
    }

    @Test fun restoredPendingSuccessIsAcknowledgedWithoutASecondAnnouncement() {
        val original = notice()
        original.present(EnrollmentState.Succeeded())
        now += 5_000L
        val recreated = notice().apply { restore(original.savedDeadline()) }
        val stillPendingInManager = EnrollmentState.Succeeded()

        val restoredSuccess = recreated.present(stillPendingInManager)

        assertTrue(restoredSuccess.visible)
        assertFalse(restoredSuccess.focusAndAnnounce)
        assertSame(stillPendingInManager, restoredSuccess.successToAcknowledge)
        assertEquals(115_000L, restoredSuccess.remainingMillis)
    }

    @Test fun invalidOrExpiredRestorationCannotReplaySuccess() {
        listOf(now, now - 1L, now + 120_001L, null).forEach { deadline ->
            val notice = notice()

            notice.restore(deadline)

            assertFalse("deadline=$deadline", notice.current().visible)
        }
    }

    @Test fun pendingAndFailureStatesImmediatelyClearSuccess() {
        listOf<EnrollmentState>(
            EnrollmentState.Exchanging,
            EnrollmentState.Failed("offline", canRetry = true),
            EnrollmentState.Failed("used", canRetry = false),
        ).forEach { terminalState ->
            val notice = notice()
            notice.present(EnrollmentState.Succeeded())

            val presentation = notice.present(terminalState)

            assertFalse(terminalState.toString(), presentation.visible)
            assertNull(notice.savedDeadline())
        }
    }

    @Test fun aNewExchangeAfterRestorationGetsFreshAttention() {
        val original = notice()
        original.present(EnrollmentState.Succeeded())
        val recreated = notice().apply { restore(original.savedDeadline()) }

        recreated.present(EnrollmentState.Exchanging)
        val newSuccess = recreated.present(EnrollmentState.Succeeded())

        assertTrue(newSuccess.focusAndAnnounce)
        assertEquals(120_000L, newSuccess.remainingMillis)
    }

    @Test(expected = IllegalArgumentException::class)
    fun displayDurationMustBePositive() {
        EnrollmentSuccessNotice({ now }, displayDurationMillis = 0L)
    }

    @Test fun deadlineSaturatesSafelyAtElapsedRealtimeLimit() {
        now = Long.MAX_VALUE - 10L
        val notice = notice()

        val presentation = notice.present(EnrollmentState.Succeeded())

        assertTrue(presentation.visible)
        assertEquals(10L, presentation.remainingMillis)
        assertEquals(Long.MAX_VALUE, notice.savedDeadline())
    }

    private fun notice() = EnrollmentSuccessNotice({ now })
}
