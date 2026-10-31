package com.vineyardfinance.sensors

import android.view.View
import android.widget.TextView
import androidx.core.widget.NestedScrollView
import com.vineyardfinance.sensors.auth.EnrollmentSuccessPresentation

/** Applies enrollment-success presentation without ever receiving token or credential material. */
internal class EnrollmentSuccessBannerUi(
    private val scrollView: NestedScrollView,
    private val banner: TextView,
    private val scrollToBanner: (NestedScrollView, TextView) -> Unit = { scroll, view ->
        scroll.smoothScrollTo(0, view.top)
    },
    private val announce: (TextView, CharSequence) -> Unit = { view, message ->
        view.announceForAccessibility(message)
    },
) {
    private var pendingAttention: Runnable? = null

    fun render(presentation: EnrollmentSuccessPresentation) {
        banner.visibility = if (presentation.visible) View.VISIBLE else View.GONE
        if (!presentation.visible) {
            cancelPendingAttention()
            banner.clearFocus()
            return
        }
        if (!presentation.focusAndAnnounce) return

        cancelPendingAttention()
        lateinit var attention: Runnable
        attention = Runnable {
            if (pendingAttention !== attention) return@Runnable
            pendingAttention = null
            if (banner.visibility != View.VISIBLE) return@Runnable
            scrollToBanner(scrollView, banner)
            banner.requestFocus()
            announce(banner, banner.text)
        }
        pendingAttention = attention
        banner.post(attention)
    }

    fun cancelPendingAttention() {
        pendingAttention?.let(banner::removeCallbacks)
        pendingAttention = null
    }
}
