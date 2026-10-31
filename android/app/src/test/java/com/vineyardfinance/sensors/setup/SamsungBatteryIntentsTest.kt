package com.vineyardfinance.sensors.setup

import android.app.Application
import android.content.Intent
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = Application::class)
class SamsungBatteryIntentsTest {
    @Test fun candidatesTargetDeviceCareBatteryFromMostToLeastSpecific() {
        val candidates = SamsungBatteryIntents.candidates()
        assertEquals(3, candidates.size)
        assertEquals(
            SamsungBatteryIntents.DEVICE_CARE_BATTERY_ACTION,
            candidates[0].action,
        )
        assertNull(candidates[0].component)
        assertEquals("com.samsung.android.lool", candidates[1].component?.packageName)
        assertEquals(
            "com.samsung.android.sm.battery.ui.BatteryActivity",
            candidates[1].component?.className,
        )
        assertEquals("com.samsung.android.lool", candidates[2].component?.packageName)
        assertEquals(
            "com.samsung.android.sm.ui.battery.BatteryActivity",
            candidates[2].component?.className,
        )
    }

    @Test fun launchUsesTheFirstCandidateThatResolvesAndLaunches() {
        val first = Intent("first")
        val second = Intent("second")
        val third = Intent("third")
        val launchedActions = mutableListOf<String?>()
        val launched = SamsungBatteryIntents.launchFirstAvailable(
            listOf(first, second, third),
            resolves = { it !== first },
            launch = {
                launchedActions.add(it.action)
                it === second
            },
        )
        assertTrue(launched)
        // The unresolvable first candidate is never launched; the resolvable
        // second candidate launches successfully and stops the walk.
        assertEquals(listOf<String?>("second"), launchedActions)
    }

    @Test fun launchFallsThroughLaunchFailuresAndReportsWhenNoneWork() {
        val candidates = listOf(Intent("a"), Intent("b"))
        val attempted = mutableListOf<String?>()
        val launched = SamsungBatteryIntents.launchFirstAvailable(
            candidates,
            resolves = { true },
            launch = {
                attempted.add(it.action)
                false
            },
        )
        assertFalse(launched)
        assertEquals(listOf<String?>("a", "b"), attempted)
    }

    @Test fun manifestDeclaresPackageVisibilityForEveryCandidate() {
        // Without matching <queries> entries, resolveActivity() returns null
        // on Android 11+ even when Device care exists, silently forcing the
        // fallback — the exact defect this feature originally shipped with.
        val manifest = java.io.File("src/main/AndroidManifest.xml").readText()
        assertTrue(manifest.contains("<queries>"))
        for (candidate in SamsungBatteryIntents.candidates()) {
            candidate.action?.let { action ->
                assertTrue(
                    "manifest <queries> must declare action $action",
                    manifest.contains("android:name=\"$action\""),
                )
            }
            candidate.component?.packageName?.let { pkg ->
                assertTrue(
                    "manifest <queries> must declare package $pkg",
                    manifest.contains("android:name=\"$pkg\""),
                )
            }
        }
    }

    @Test fun launchReportsFalseWhenNothingResolves() {
        val launched = SamsungBatteryIntents.launchFirstAvailable(
            SamsungBatteryIntents.candidates(),
            resolves = { false },
            launch = { error("must not launch an unresolvable intent") },
        )
        assertFalse(launched)
    }
}
