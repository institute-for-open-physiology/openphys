package com.vineyardfinance.sensors

import android.app.Application
import android.content.Context
import android.view.LayoutInflater
import android.widget.Button
import android.widget.CheckBox
import android.widget.TextView
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = Application::class)
class SamsungBackgroundGuidanceUiTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test fun visibleCopyNamesBothSamsungPathsAndTruthfulManualConfirmation() {
        val root = LayoutInflater.from(context).inflate(R.layout.activity_setup, null)
        val guidance = root.findViewById<TextView>(R.id.manufacturerGuidance).text.toString()
        val confirmation = root.findViewById<CheckBox>(R.id.manufacturerConfirm).text.toString()
        val settingsButton = root.findViewById<Button>(R.id.manufacturerSettings).text.toString()
        val sleepingButton = root.findViewById<Button>(R.id.manufacturerSleepingApps).text.toString()

        assertTrue(guidance.contains("Open Samsung app settings → Battery → Unrestricted"))
        assertTrue(
            guidance.contains(
                "Open Never sleeping apps settings → Background usage limits → " +
                    "Never sleeping apps → add Training Sensors",
            ),
        )
        assertTrue(guidance.contains("Use the buttons above for each step"))
        assertTrue(
            guidance.contains(
                "manual path: Settings → Battery and device care → Battery → Background usage limits",
            ),
        )
        assertTrue(guidance.contains("cannot reliably verify"))
        assertTrue(guidance.contains("only after completing both steps"))
        assertEquals(
            "I set Battery to Unrestricted and reviewed Never sleeping apps",
            confirmation,
        )
        assertEquals("Open Samsung app settings", settingsButton)
        assertEquals("Open Never sleeping apps settings", sleepingButton)
    }
}
