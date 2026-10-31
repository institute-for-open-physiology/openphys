package com.vineyardfinance.sensors.setup

import android.Manifest
import android.app.Application
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class RequiredPermissionsTest {
    private val context: Application = ApplicationProvider.getApplicationContext()

    @Test fun locationIsNeverInTheAlwaysRequiredListOnModernAndroid() {
        // Location permissions belong exclusively to the opt-in flow; on 31+
        // BLE scanning no longer needs them and they must not be demanded.
        val modern = RequiredPermissions.list(sdkInt = 34)
        assertFalse(Manifest.permission.ACCESS_FINE_LOCATION in modern)
        assertFalse(Manifest.permission.ACCESS_COARSE_LOCATION in modern)
        assertFalse(Manifest.permission.ACCESS_BACKGROUND_LOCATION in modern)
    }

    @Test fun legacyBleStillRequiresForegroundLocationButNeverBackground() {
        val legacy = RequiredPermissions.list(sdkInt = 28)
        assertTrue(Manifest.permission.ACCESS_FINE_LOCATION in legacy)
        assertTrue(Manifest.permission.ACCESS_COARSE_LOCATION in legacy)
        assertFalse(Manifest.permission.ACCESS_BACKGROUND_LOCATION in legacy)
    }

    @Test fun optInLocationPermissionsAreFineAndCoarse() {
        assertEquals(
            listOf(
                Manifest.permission.ACCESS_FINE_LOCATION,
                Manifest.permission.ACCESS_COARSE_LOCATION,
            ),
            RequiredPermissions.location(),
        )
    }

    @Test fun missingLocationReportsUngrantedPermissions() {
        // Robolectric grants nothing by default.
        assertEquals(RequiredPermissions.location(), RequiredPermissions.missingLocation(context))
    }

    @Test fun missingReportsAllRequiredPermissionsWhenNoneAreGranted() {
        assertEquals(RequiredPermissions.list(), RequiredPermissions.missing(context))
    }

    @Test fun defaultSdkOverloadsFollowTheRunningPlatform() {
        assertEquals(
            RequiredPermissions.backgroundLocation(android.os.Build.VERSION.SDK_INT),
            RequiredPermissions.backgroundLocation(),
        )
        assertEquals(
            RequiredPermissions.backgroundLocationGranted(context, android.os.Build.VERSION.SDK_INT),
            RequiredPermissions.backgroundLocationGranted(context),
        )
    }

    @Test fun backgroundLocationExistsOnlyFromAndroid10() {
        assertNull(RequiredPermissions.backgroundLocation(sdkInt = 28))
        assertEquals(
            Manifest.permission.ACCESS_BACKGROUND_LOCATION,
            RequiredPermissions.backgroundLocation(sdkInt = 29),
        )
    }

    @Test fun backgroundLocationGrantedFollowsThePlatformCheck() {
        // No such permission before Android 10: trivially satisfied.
        assertTrue(RequiredPermissions.backgroundLocationGranted(context, sdkInt = 28))
        // Not granted in the bare Robolectric application.
        assertFalse(RequiredPermissions.backgroundLocationGranted(context, sdkInt = 34))
    }
}
