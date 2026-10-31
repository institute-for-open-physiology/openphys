package com.vineyardfinance.sensors

import android.app.Application
import android.app.Activity
import android.content.Context
import android.graphics.Typeface
import android.graphics.drawable.ColorDrawable
import android.os.Looper
import android.view.LayoutInflater
import android.view.View
import android.widget.TextView
import androidx.core.content.ContextCompat
import androidx.core.widget.NestedScrollView
import androidx.test.core.app.ApplicationProvider
import com.vineyardfinance.sensors.auth.EnrollmentState
import com.vineyardfinance.sensors.auth.EnrollmentSuccessNotice
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Robolectric
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.annotation.LooperMode

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = Application::class)
@LooperMode(LooperMode.Mode.PAUSED)
class EnrollmentSuccessBannerUiTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test fun bannerIsInitiallyHiddenProminentAccessibleAndContainsOnlyStaticCopy() {
        val root = inflate()
        val banner = root.findViewById<TextView>(R.id.enrollmentSuccess)
        val copy = banner.text.toString()

        assertEquals(View.GONE, banner.visibility)
        assertEquals(
            "Phone connected. Continue setup below, then tap Start durable collection.",
            copy,
        )
        assertTrue(banner.isFocusable)
        assertTrue(banner.isFocusableInTouchMode)
        assertTrue(banner.isAccessibilityHeading)
        assertTrue(banner.isScreenReaderFocusable)
        assertEquals(View.IMPORTANT_FOR_ACCESSIBILITY_YES, banner.importantForAccessibility)
        assertEquals(Typeface.BOLD, banner.typeface.style)
        assertEquals(ContextCompat.getColor(context, R.color.up), banner.currentTextColor)
        assertEquals(
            ContextCompat.getColor(context, R.color.surface),
            (banner.background as ColorDrawable).color,
        )
        assertTrue(banner.textSize > root.findViewById<TextView>(R.id.enrollmentStatus).textSize)
        assertFalse(copy.contains("http", ignoreCase = true))
        assertFalse(copy.contains("token", ignoreCase = true))
        assertFalse(copy.contains("credential", ignoreCase = true))
        assertFalse(copy.contains("installation id", ignoreCase = true))
        assertFalse(copy.contains('#'))
        assertFalse(copy.contains('%'))
        assertFalse(Regex("[A-Za-z0-9_-]{43,}").containsMatchIn(copy))
    }

    @Test fun genuineSuccessScrollsFocusesAndAnnouncesExactlyOnceAfterAcknowledgement() {
        val root = inflate()
        val banner = root.findViewById<TextView>(R.id.enrollmentSuccess)
        var scrollCount = 0
        val announcements = mutableListOf<String>()
        val ui = EnrollmentSuccessBannerUi(
            root,
            banner,
            scrollToBanner = { _, _ -> scrollCount++ },
            announce = { _, message -> announcements += message.toString() },
        )
        var now = 1_000L
        val notice = EnrollmentSuccessNotice({ now })

        ui.render(notice.present(EnrollmentState.Succeeded()))
        now += 1L
        ui.render(notice.present(EnrollmentState.Idle))
        shadowOf(Looper.getMainLooper()).idle()

        assertEquals(View.VISIBLE, banner.visibility)
        assertTrue(banner.isFocused)
        assertEquals(1, scrollCount)
        assertEquals(listOf(context.getString(R.string.enrollment_success)), announcements)

        ui.render(notice.present(EnrollmentState.Idle))
        shadowOf(Looper.getMainLooper()).idle()
        assertEquals(1, scrollCount)
        assertEquals(1, announcements.size)
    }

    @Test fun productionRendererMovesFocusAndAnnouncesWithoutSecretInputs() {
        val root = inflate()
        val banner = root.findViewById<TextView>(R.id.enrollmentSuccess)
        val ui = EnrollmentSuccessBannerUi(root, banner)
        val notice = EnrollmentSuccessNotice({ 1_000L })

        ui.render(notice.present(EnrollmentState.Succeeded()))
        shadowOf(Looper.getMainLooper()).idle()

        assertEquals(View.VISIBLE, banner.visibility)
        assertTrue(banner.isFocused)
    }

    @Test fun recreationShowsFeedbackWithoutMovingFocusOrAnnouncingAgain() {
        val root = inflate()
        val banner = root.findViewById<TextView>(R.id.enrollmentSuccess)
        var attentionCalls = 0
        val ui = EnrollmentSuccessBannerUi(
            root,
            banner,
            scrollToBanner = { _, _ -> attentionCalls++ },
            announce = { _, _ -> attentionCalls++ },
        )
        var now = 5_000L
        val original = EnrollmentSuccessNotice({ now })
        original.present(EnrollmentState.Succeeded())
        now += 10_000L
        val recreated = EnrollmentSuccessNotice({ now }).apply { restore(original.savedDeadline()) }

        ui.render(recreated.current())
        shadowOf(Looper.getMainLooper()).idle()

        assertEquals(View.VISIBLE, banner.visibility)
        assertFalse(banner.isFocused)
        assertEquals(0, attentionCalls)
    }

    @Test fun pendingAndFailureHideTheBannerAndCancelStaleAttention() {
        listOf<EnrollmentState>(
            EnrollmentState.Exchanging,
            EnrollmentState.Failed("offline", canRetry = true),
            EnrollmentState.Failed("used", canRetry = false),
        ).forEach { nextState ->
            val root = inflate()
            val banner = root.findViewById<TextView>(R.id.enrollmentSuccess)
            var attentionCalls = 0
            val ui = EnrollmentSuccessBannerUi(
                root,
                banner,
                scrollToBanner = { _, _ -> attentionCalls++ },
                announce = { _, _ -> attentionCalls++ },
            )
            val notice = EnrollmentSuccessNotice({ 5_000L })

            ui.render(notice.present(EnrollmentState.Succeeded()))
            ui.render(notice.present(nextState))
            shadowOf(Looper.getMainLooper()).idle()

            assertEquals(nextState.toString(), View.GONE, banner.visibility)
            assertFalse(nextState.toString(), banner.isFocused)
            assertEquals(nextState.toString(), 0, attentionCalls)
        }
    }

    private fun inflate(): NestedScrollView {
        val activity = Robolectric.buildActivity(Activity::class.java).setup().get()
        return (LayoutInflater.from(activity).inflate(R.layout.activity_setup, null) as NestedScrollView).also {
            activity.setContentView(it)
        }
    }
}
