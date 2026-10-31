package com.vineyardfinance.sensors

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.vineyardfinance.sensors.collector.H10Identity
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = Application::class)
class FreshInstallTargetTest {
    @Test fun freshInstallHasNoStrapSerialAndCannotSilentlyScanForSomeoneElses() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val settings = AppSettings(context)
        // No built-in default: a tester's fresh install must not come
        // prefilled with another person's H10 serial.
        assertEquals("", settings.target)
        // An empty target is invalid, so the setup checklist gates on
        // "Enter the serial printed on the Polar H10" instead of scanning.
        assertTrue(runCatching { H10Identity.configuredSerial(settings.target) }.isFailure)
        // Entering a serial still works and normalizes as before.
        settings.target = "1a2b3c4d"
        assertEquals("1A2B3C4D", settings.target)
    }
}
