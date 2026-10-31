package com.vineyardfinance.sensors.collector

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.content.ContextCompat
import com.vineyardfinance.sensors.SensorsApplication

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != Intent.ACTION_BOOT_COMPLETED && intent.action != Intent.ACTION_MY_PACKAGE_REPLACED) return
        val graph = (context.applicationContext as SensorsApplication).graph
        val permitted = Build.VERSION.SDK_INT < 31 ||
            (ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED &&
                ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED)
        val setup = graph.setup.inspect(CollectorStatus.updates.value)
        val shouldResume = CollectorAdmission.shouldResumeAfterBoot(
            graph.settings.collectionEnabled,
            graph.credentials.currentCredential() != null,
            permitted,
            setup,
        )
        if (shouldResume) {
            CollectorService.start(context)
        }
    }
}
