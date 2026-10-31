package com.vineyardfinance.sensors.setup

import android.Manifest
import android.app.ActivityManager
import android.app.Application
import android.app.NotificationManager
import android.bluetooth.BluetoothManager
import android.content.pm.PackageManager
import android.os.Build
import android.os.PowerManager
import androidx.core.content.ContextCompat
import com.vineyardfinance.sensors.AppSettings
import com.vineyardfinance.sensors.auth.DeviceCredentialProvider
import com.vineyardfinance.sensors.collector.CollectorNotificationChannel
import com.vineyardfinance.sensors.collector.CollectorSnapshot
import com.vineyardfinance.sensors.collector.H10Identity
import com.vineyardfinance.sensors.data.AppIdentity

class AndroidSetupInspector(
    private val application: Application,
    private val settings: AppSettings,
    private val credentials: DeviceCredentialProvider,
    private val identity: AppIdentity,
) {
    init {
        CollectorNotificationChannel.ensure(application)
    }

    fun inspect(status: CollectorSnapshot, allowDebugCredential: Boolean = false): SetupReport {
        val bluetooth = application.getSystemService(BluetoothManager::class.java).adapter
        val notifications = application.getSystemService(NotificationManager::class.java)
        val power = application.getSystemService(PowerManager::class.java)
        val activity = application.getSystemService(ActivityManager::class.java)
        val manufacturer = Build.MANUFACTURER
        val nowMs = System.currentTimeMillis()
        return SetupChecklist.evaluate(
            SetupFacts(
                credentialAvailable = allowDebugCredential || credentials.connected.value,
                installationIdValid = runCatching { java.util.UUID.fromString(identity.installationId) }.isSuccess,
                bluetoothAvailable = bluetooth != null,
                bluetoothEnabled = runCatching { bluetooth?.isEnabled == true }.getOrDefault(false),
                nearbyPermissionsGranted = nearbyPermissionsGranted(),
                notificationPermissionGranted = Build.VERSION.SDK_INT < 33 || granted(Manifest.permission.POST_NOTIFICATIONS),
                notificationsEnabled = notifications.areNotificationsEnabled(),
                notificationChannelEnabled = CollectorNotificationChannel.isEnabled(application),
                batteryOptimizationExempt = power.isIgnoringBatteryOptimizations(application.packageName),
                backgroundRestricted = activity.isBackgroundRestricted,
                manufacturerPolicyRequired = ManufacturerBackgroundPolicy.forManufacturer(manufacturer).required,
                manufacturerIncompleteDetail = ManufacturerBackgroundPolicy.forManufacturer(manufacturer).incompleteDetail,
                manufacturerConfirmedDetail = ManufacturerBackgroundPolicy.forManufacturer(manufacturer).confirmedDetail,
                manufacturerBackgroundConfirmed = settings.manufacturerBackgroundConfirmed(manufacturer),
                h10TargetValid = runCatching { H10Identity.configuredSerial(settings.target) }.isSuccess,
                collectionEnabled = settings.collectionEnabled,
                collectorConnected = status.connected,
                ecgFresh = fresh(status.ecgUnixNs, nowMs, STREAM_FRESH_MS),
                accFresh = fresh(status.accUnixNs, nowMs, STREAM_FRESH_MS),
                hrFresh = fresh(status.hrUnixNs, nowMs, STREAM_FRESH_MS),
                storageHealthy = status.backlogKnown && status.backlogStatusError == null &&
                    status.state != "Collection failed" && !isStorageError(status.lastError),
                cloudAckFresh = status.lastAckUnixMs?.let { nowMs - it in 0..ACK_FRESH_MS } == true,
            ),
        )
    }

    private fun nearbyPermissionsGranted(): Boolean = if (Build.VERSION.SDK_INT >= 31) {
        granted(Manifest.permission.BLUETOOTH_SCAN) && granted(Manifest.permission.BLUETOOTH_CONNECT)
    } else {
        granted(Manifest.permission.ACCESS_COARSE_LOCATION) && granted(Manifest.permission.ACCESS_FINE_LOCATION)
    }

    private fun granted(permission: String): Boolean =
        ContextCompat.checkSelfPermission(application, permission) == PackageManager.PERMISSION_GRANTED

    private fun fresh(unixNs: Long?, nowMs: Long, thresholdMs: Long): Boolean =
        unixNs?.let { nowMs - it / 1_000_000L in 0..thresholdMs } == true

    private fun isStorageError(error: String?): Boolean = error?.let { message ->
        listOf("storage", "local queue", "database", "sqlite", "disk").any {
            message.contains(it, ignoreCase = true)
        }
    } == true

    companion object {
        const val STREAM_FRESH_MS = 10_000L
        const val ACK_FRESH_MS = 120_000L
    }
}
