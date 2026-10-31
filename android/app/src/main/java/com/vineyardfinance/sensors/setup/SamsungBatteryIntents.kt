package com.vineyardfinance.sensors.setup

import android.content.Intent

/**
 * Ordered launch candidates for Samsung's "Never sleeping apps" screen.
 *
 * One UI has no public, stable intent for the Background usage limits page,
 * so the guided setup tries known Device care entry points from most to
 * least specific and the caller falls back to the app's own settings page
 * when none resolves. Every candidate lands the user inside Device care's
 * Battery area, from which "Background usage limits → Never sleeping apps →
 * add Training Sensors" is one or two taps following the on-screen text.
 */
object SamsungBatteryIntents {
    const val DEVICE_CARE_BATTERY_ACTION = "com.samsung.android.sm.ACTION_BATTERY"

    fun candidates(): List<Intent> = listOf(
        Intent(DEVICE_CARE_BATTERY_ACTION),
        Intent().setClassName(
            "com.samsung.android.lool",
            "com.samsung.android.sm.battery.ui.BatteryActivity",
        ),
        Intent().setClassName(
            "com.samsung.android.lool",
            "com.samsung.android.sm.ui.battery.BatteryActivity",
        ),
    )

    /**
     * Launches the first candidate that both resolves and launches, in
     * order. Returns false when none could be launched so the caller can
     * fall back instead of dead-ending the guided setup.
     */
    fun launchFirstAvailable(
        candidates: List<Intent>,
        resolves: (Intent) -> Boolean,
        launch: (Intent) -> Boolean,
    ): Boolean {
        for (candidate in candidates) {
            if (resolves(candidate) && launch(candidate)) {
                return true
            }
        }
        return false
    }
}
