package com.vineyardfinance.sensors.seca

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import androidx.core.content.ContextCompat
import com.vineyardfinance.sensors.SensorsApplication

class SecaBootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action !in listOf(Intent.ACTION_BOOT_COMPLETED, Intent.ACTION_MY_PACKAGE_REPLACED)) return
        val graph = (context.applicationContext as SensorsApplication).graph
        SecaUploadWorker.enqueue(context)
        com.vineyardfinance.sensors.network.UploadWorker.enqueue(context)
        if (!graph.secaSettings.desired) return
        val allowed = Build.VERSION.SDK_INT < 31 || ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
        if (!allowed) {
            SecaStatus.update { it.copy(error = "Grant Nearby devices permission to resume seca recording") }
            return
        }
        try { SecaService.start(context) }
        catch (error: Exception) {
            Log.e("SecaBoot", "Cannot restart seca recording", error)
            SecaStatus.update { it.copy(error = "Open seca ECG to resume: ${error.message}") }
        }
    }
}
