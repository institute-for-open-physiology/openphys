package com.vineyardfinance.sensors.setup

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.content.ContextCompat

/**
 * The single authoritative list of runtime permissions the collector needs,
 * shared by the main screen and the onboarding wizard so neither can drift.
 */
object RequiredPermissions {
    fun list(sdkInt: Int = Build.VERSION.SDK_INT): List<String> = buildList {
        if (sdkInt >= 31) {
            add(Manifest.permission.BLUETOOTH_SCAN)
            add(Manifest.permission.BLUETOOTH_CONNECT)
        } else {
            add(Manifest.permission.ACCESS_COARSE_LOCATION)
            add(Manifest.permission.ACCESS_FINE_LOCATION)
        }
        if (sdkInt >= 33) add(Manifest.permission.POST_NOTIFICATIONS)
    }

    fun missing(context: Context): List<String> = list().filter {
        ContextCompat.checkSelfPermission(context, it) != PackageManager.PERMISSION_GRANTED
    }

    /**
     * Location permissions are deliberately NOT part of [list]: they are
     * requested on demand, only when the user turns on the per-installation
     * location opt-in (docs/gps_tracking_proposal_20260808.md).
     */
    fun location(): List<String> = listOf(
        Manifest.permission.ACCESS_FINE_LOCATION,
        Manifest.permission.ACCESS_COARSE_LOCATION,
    )

    fun missingLocation(context: Context): List<String> = location().filter {
        ContextCompat.checkSelfPermission(context, it) != PackageManager.PERMISSION_GRANTED
    }

    /**
     * "Allow all the time" background location; keeps GPS recording when the
     * collector restarts without the app being opened (for example after a
     * phone reboot). Null below Android 10, where no such permission exists.
     */
    fun backgroundLocation(sdkInt: Int = Build.VERSION.SDK_INT): String? =
        if (sdkInt >= 29) Manifest.permission.ACCESS_BACKGROUND_LOCATION else null

    fun backgroundLocationGranted(context: Context, sdkInt: Int = Build.VERSION.SDK_INT): Boolean {
        val permission = backgroundLocation(sdkInt) ?: return true
        return ContextCompat.checkSelfPermission(context, permission) == PackageManager.PERMISSION_GRANTED
    }
}
