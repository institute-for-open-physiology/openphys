package com.vineyardfinance.sensors

import android.app.Application
import android.content.Context
import android.view.LayoutInflater
import android.view.View
import android.widget.EditText
import android.widget.TextView
import androidx.core.widget.NestedScrollView
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(
    sdk = [34],
    qualifiers = "w384dp-h756dp-xxhdpi",
    application = Application::class,
)
class StatusPanelLayoutTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test
    fun statusViewportsKeepFollowingControlsStableAndLongTextAccessible() {
        val root = LayoutInflater.from(context).inflate(R.layout.activity_setup, null)
        val statePanel = root.findViewById<NestedScrollView>(R.id.statePanel)
        val state = root.findViewById<TextView>(R.id.state)
        val telemetryPanel = root.findViewById<NestedScrollView>(R.id.telemetryPanel)
        val telemetry = root.findViewById<TextView>(R.id.telemetry)
        val target = root.findViewById<EditText>(R.id.target)
        val shortState = "Streaming\nECG + ACC + HR"
        val shortStatus = listOf(
            "Connected: true",
            "RSSI: -72 dBm  Battery: 75%",
            "Freshness: ECG 45 ms, ACC 8 ms, HR 821 ms",
            "Durable phone backlog: 3 records",
            "Upload: Durable ACK through 365714",
        ).joinToString("\n")
        val longError = buildString {
            while (length < 240) append("UPLOAD ERROR DETAIL\n")
        }.take(240)
        val longDetail = buildString {
            while (length < 240) append("CONNECTION FAILURE DETAIL\n")
        }.take(240)
        val longState = "Reconnecting\n$longDetail"
        val longStatus = "$shortStatus\nLast error: $longError"

        state.text = shortState
        telemetry.text = shortStatus
        layoutAtGalaxySize(root)
        val expectedStateHeight = context.resources.getDimensionPixelSize(R.dimen.collector_state_panel_height)
        val expectedTelemetryHeight = context.resources.getDimensionPixelSize(R.dimen.telemetry_panel_height)
        val shortTargetTop = target.top

        assertEquals(expectedStateHeight, statePanel.measuredHeight)
        assertEquals(expectedTelemetryHeight, telemetryPanel.measuredHeight)
        assertEquals(shortState, state.text.toString())
        assertEquals(shortStatus, telemetry.text.toString())
        assertNull(state.ellipsize)
        assertNull(telemetry.ellipsize)

        state.text = longState
        telemetry.text = longStatus
        layoutAtGalaxySize(root)

        assertEquals(expectedStateHeight, statePanel.measuredHeight)
        assertEquals(expectedTelemetryHeight, telemetryPanel.measuredHeight)
        assertEquals(shortTargetTop, target.top)
        assertEquals(longState, state.text.toString())
        assertEquals(longStatus, telemetry.text.toString())
        assertEquals(View.IMPORTANT_FOR_ACCESSIBILITY_YES, state.importantForAccessibility)
        assertEquals(View.IMPORTANT_FOR_ACCESSIBILITY_YES, telemetry.importantForAccessibility)
        assertTrue(statePanel.isVerticalScrollBarEnabled)
        assertTrue(statePanel.isNestedScrollingEnabled)
        assertTrue(state.measuredHeight > statePanel.measuredHeight)
        assertTrue(statePanel.canScrollVertically(1))
        assertTrue(telemetryPanel.isVerticalScrollBarEnabled)
        assertTrue(telemetryPanel.isNestedScrollingEnabled)
        assertTrue(
            "long telemetry height=${telemetry.measuredHeight}, panel height=${telemetryPanel.measuredHeight}, width=${telemetry.measuredWidth}",
            telemetry.measuredHeight > telemetryPanel.measuredHeight,
        )
        assertTrue(telemetryPanel.canScrollVertically(1))
    }

    private fun layoutAtGalaxySize(root: View) {
        val metrics = context.resources.displayMetrics
        val width = (384 * metrics.density).toInt()
        val height = (756 * metrics.density).toInt()
        root.measure(
            View.MeasureSpec.makeMeasureSpec(width, View.MeasureSpec.EXACTLY),
            View.MeasureSpec.makeMeasureSpec(height, View.MeasureSpec.EXACTLY),
        )
        root.layout(0, 0, width, height)
    }
}
