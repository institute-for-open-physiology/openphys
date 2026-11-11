package com.vineyardfinance.sensors.collector

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothManager
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.app.usage.UsageStatsManager
import android.content.Context
import android.net.ConnectivityManager
import android.os.ParcelUuid
import android.os.BatteryManager
import android.os.PowerManager
import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.AppSettings
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.data.RecordJson
import com.vineyardfinance.sensors.data.RecordJson.nullable
import com.vineyardfinance.sensors.network.UploadLoopVitals
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withContext
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.random.Random

@SuppressLint("MissingPermission")
class H10Collector(
    private val context: Context,
    private val settings: AppSettings,
    private val writer: RawRecordWriter,
    private val random: Random = Random.Default,
) {
    private val bluetoothManager = context.getSystemService(BluetoothManager::class.java)
    private val batteryManager = context.getSystemService(BatteryManager::class.java)
    private val powerManager = context.getSystemService(PowerManager::class.java)
    private val adapter: BluetoothAdapter get() = bluetoothManager.adapter ?: error("Bluetooth adapter unavailable")

    suspend fun run() {
        val configuredSerial = H10Identity.configuredSerial(settings.target)
        check(writer.baseContext.physicalDeviceId == H10Identity.physicalDeviceId(configuredSerial)) {
            "capture provenance does not match configured H10"
        }
        writer.append("session_started", RecordJson.body {
            addProperty("software_name", "Training Sensors Android")
            addProperty("software_version", BuildConfig.VERSION_NAME)
            add("git_commit", JsonNull.INSTANCE)
            addProperty("hostname", android.os.Build.MODEL)
            addProperty("pid", android.os.Process.myPid())
            addProperty("target", configuredSerial)
            addProperty("configured_h10_serial", configuredSerial)
            addProperty("physical_device_id", writer.baseContext.physicalDeviceId)
            addProperty("started_reason", "foreground_service_start")
            addProperty("config_json", "{\"ecg_hz\":130,\"acc_hz\":200,\"watchdog_ms\":2000}")
        })
        try {
            while (currentCoroutineContext().isActive && settings.collectionEnabled) {
                if (!adapter.isEnabled) {
                    CollectorStatus.update { it.transitionTo("Bluetooth off", "Waiting for Bluetooth") }
                    writer.event("bluetooth_adapter_off", "error", "Bluetooth adapter is disabled")
                    delay(1_000)
                    continue
                }
                var connection: GattConnection? = null
                try {
                    val device = scanForTarget(configuredSerial)
                    writer.connectionId = UUID.randomUUID().toString()
                    connection = GattConnection(context, writer) { throwFromCallback(it) }
                    CollectorStatus.update { it.transitionTo("Connecting", device.address) }
                    connection.connect(device)
                    connection.ensureBonded()
                    connection.negotiateMtu()
                    connection.discoverServices()
                    initialize(connection, configuredSerial)
                    stream(connection)
                } catch (error: Throwable) {
                    handleConnectionCycleFailure(error) {
                        val detail = safe(error)
                        writer.event("connection_cycle_failed", "error", detail, RecordJson.fields(mapOf("connection_id" to writer.connectionId)))
                        CollectorStatus.update { it.transitionTo("Reconnecting", detail, error = detail) }
                    }
                } finally {
                    withContext(NonCancellable) {
                        try {
                            if (connection != null) {
                                runCatching {
                                    withTimeout(CONNECTION_CLEANUP_TIMEOUT_MS) {
                                        connection.disconnect("cleanup_after_connection_cycle")
                                    }
                                }.onFailure {
                                    runCatching { writer.event("connection_cleanup_failed", "error", safe(it)) }
                                }
                            }
                        } finally {
                            connection?.close()
                        }
                    }
                    writer.signals?.disconnect()
                    writer.connectionId = null
                    writer.peripheralId = null
                    CollectorStatus.update { it.copy(connected = false) }
                }
                delay(200 + random.nextLong(0, 301))
            }
        } finally {
            withContext(NonCancellable) {
                withTimeout(SESSION_END_TIMEOUT_MS) {
                    writer.append("session_ended", RecordJson.body {
                        addProperty("reason", if (settings.collectionEnabled) "service_stopped" else "user_disabled")
                        addProperty("message", "Android foreground collector stopped")
                    })
                }
            }
        }
    }

    private suspend fun scanForTarget(target: String): BluetoothDevice {
        val scanner = adapter.bluetoothLeScanner ?: throw IOException("BLE scanner unavailable")
        val attemptId = UUID.randomUUID().toString()
        writer.append("scan_attempt", RecordJson.body {
            addProperty("attempt_id", attemptId); addProperty("target", target); addProperty("scan_timeout_ms", SCAN_TIMEOUT_MS)
            add("service_uuids", JsonArray())
        })
        CollectorStatus.update { it.transitionTo("Scanning", "Polar H10 $target") }
        val results = Channel<Result<ScanResult>>(Channel.UNLIMITED)
        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) { results.trySend(Result.success(result)) }
            override fun onBatchScanResults(batchResults: MutableList<ScanResult>) { batchResults.forEach { results.trySend(Result.success(it)) } }
            override fun onScanFailed(errorCode: Int) { results.trySend(Result.failure(IOException("Android scan error $errorCode"))) }
        }
        // Android suspends unfiltered scans when the screen turns off. A concrete name filter is
        // required for the locked-screen foreground-service acceptance path.
        val expectedName = H10Identity.advertisedName(target)
        scanner.startScan(
            listOf(ScanFilter.Builder().setDeviceName(expectedName).build()),
            ScanSettings.Builder().setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY).build(),
            callback,
        )
        try {
            // Each scan attempt is bounded; the outer loop retries aggressively with jitter.
            return withTimeout(SCAN_TIMEOUT_MS) {
                while (true) {
                    val result = results.receive().getOrThrow()
                    val name = result.scanRecord?.deviceName ?: runCatching { result.device.name }.getOrNull()
                    val matched = name?.equals(expectedName, ignoreCase = true) == true
                    writer.append("scan_result", RecordJson.body {
                        addProperty("attempt_id", attemptId)
                        addProperty("peripheral_id", result.device.address.lowercase())
                        nullable("local_name", name)
                        addProperty("rssi_dbm", result.rssi)
                        val manufacturer = result.scanRecord?.let { record ->
                            if (record.manufacturerSpecificData.size() > 0) record.manufacturerSpecificData.valueAt(0) else null
                        }
                        if (manufacturer == null) add("manufacturer_data_b64", JsonNull.INSTANCE)
                        else addProperty("manufacturer_data_b64", RecordJson.payload(manufacturer))
                        add("service_uuids", JsonArray().apply { result.scanRecord?.serviceUuids.orEmpty().map(ParcelUuid::getUuid).forEach { add(it.toString().lowercase()) } })
                        addProperty("matched", matched)
                        addProperty("configured_h10_serial", H10Identity.configuredSerial(target))
                        addProperty("configured_physical_device_id", writer.baseContext.physicalDeviceId)
                    })
                    if (matched) return@withTimeout result.device
                }
                @Suppress("UNREACHABLE_CODE") error("scan ended")
            }
        } catch (error: Throwable) {
            writer.append("scan_error", RecordJson.body {
                addProperty("attempt_id", attemptId); addProperty("stage", "android_ble_scan"); addProperty("error", safe(error))
            })
            throw error
        } finally {
            scanner.stopScan(callback)
            results.close()
        }
    }

    private suspend fun initialize(connection: GattConnection, configuredSerial: String) {
        val deviceInfo = linkedMapOf<String, String?>()
        val info = listOf(
            H10Protocol.MANUFACTURER_NAME to ("manufacturer" to "manufacturer_name"),
            H10Protocol.MODEL_NUMBER to ("model" to "model_number"),
            H10Protocol.SERIAL_NUMBER to ("serial" to "serial_number"),
            H10Protocol.HARDWARE_REVISION to ("hardware_revision" to "hardware_revision"),
            H10Protocol.FIRMWARE_REVISION to ("firmware_revision" to "firmware_revision"),
            H10Protocol.SOFTWARE_REVISION to ("software_revision" to "software_revision"),
        )
        for ((uuid, labels) in info) {
            deviceInfo[labels.first] = connection.characteristic(uuid)?.let { characteristic ->
                recoverableResult { connection.read(characteristic, labels.second).toString(Charsets.UTF_8).trim('\u0000') }.getOrNull()
            }
        }
        val identity = try {
            H10Identity.verifyObservedSerial(configuredSerial, deviceInfo["serial"])
        } catch (error: IllegalArgumentException) {
            writer.event(
                "h10_identity_mismatch",
                "critical",
                error.message ?: "H10 identity verification failed",
                RecordJson.fields(
                    mapOf(
                        "configured_h10_serial" to configuredSerial,
                        "observed_h10_serial" to deviceInfo["serial"],
                        "configured_physical_device_id" to writer.baseContext.physicalDeviceId,
                    ),
                ),
            )
            throw IOException(error.message, error)
        }
        writer.event(
            "h10_identity_verified",
            "info",
            "Device Information serial matches configured H10",
            RecordJson.fields(
                mapOf(
                    "configured_h10_serial" to identity.configuredSerial,
                    "observed_h10_serial" to identity.observedSerial,
                    "physical_device_id" to identity.physicalDeviceId,
                    "ble_peripheral_id" to writer.peripheralId,
                ),
            ),
        )
        readBattery(connection)
        writer.append("service_discovery_summary", connection.serviceSummary(deviceInfo))
        trySetLocalTime(connection)

        val hr = required(connection, H10Protocol.HR_MEASUREMENT)
        val control = required(connection, H10Protocol.PMD_CONTROL)
        val pmdData = required(connection, H10Protocol.PMD_DATA)
        connection.subscribe(hr, "heart_rate_measurement")
        connection.subscribe(control, "pmd_control")
        connection.subscribe(pmdData, "pmd_data")
        connection.read(control, "pmd_features")

        bestEffortStop(connection, control, H10Protocol.ECG_STOP, "pre_start_stop_ecg")
        bestEffortStop(connection, control, H10Protocol.ACC_STOP, "pre_start_stop_acc")
        delay(100)
        startStream(connection, control, H10Protocol.ECG_START, "pmd_start_ecg")
        delay(100)
        startStream(connection, control, H10Protocol.ACC_START, "pmd_start_acc")
        writer.event("pmd_stream_start_confirmed", "info", "ecg+acc", RecordJson.fields(mapOf("ecg_hz" to 130, "acc_hz" to 200)))
        CollectorStatus.update {
            it.transitionTo("Streaming", "ECG + ACC + HR", connected = true).copy(
                ecgUnixNs = null,
                accUnixNs = null,
                hrUnixNs = null,
            )
        }
    }

    private suspend fun trySetLocalTime(connection: GattConnection) {
        val characteristic = connection.characteristic(H10Protocol.PSFTP_MTU)
        if (characteristic == null) {
            writer.event("psftp_set_local_time_unavailable", "warn", "PS-FTP characteristic absent")
            return
        }
        if (!connection.isBonded()) {
            throw IOException("H10 bond is required before PS-FTP local-time setting")
        }
        val operationId = UUID.randomUUID().toString()
        val startNs = System.nanoTime()
        try {
            connection.subscribe(characteristic, "psftp_mtu")
            val time = H10Protocol.psftpLocalTime(OffsetDateTime.now())
            writer.append("psftp_set_local_time_attempt", RecordJson.body {
                addProperty("operation_id", operationId); addProperty("local_time_iso8601", time.iso8601)
                addProperty("local_time_unix_ns", time.unixNs); addProperty("tz_offset_minutes", time.offsetMinutes)
                add("dst_offset_minutes", JsonNull.INSTANCE); addProperty("request_payload_b64", RecordJson.payload(time.parameters))
                addProperty("control_characteristic_uuid", H10Protocol.PSFTP_MTU.toString().lowercase())
                addProperty("mtu_characteristic_uuid", H10Protocol.PSFTP_MTU.toString().lowercase())
            })
            time.packets.forEach { connection.write(characteristic, it, "psftp_set_local_time_packet") }
            val assembler = H10Protocol.PsftpResponseAssembler()
            var response: ByteArray? = null
            val deadline = System.nanoTime() + PSFTP_TIMEOUT_MS * 1_000_000L
            while (response == null) {
                val remainingMs = ((deadline - System.nanoTime()) / 1_000_000L).coerceAtLeast(1)
                response = assembler.accept(connection.awaitNotification(H10Protocol.PSFTP_MTU, remainingMs)).getOrThrow()
            }
            writer.append("psftp_set_local_time_result", psftpResult(operationId, true, 0, response, null, elapsedMs(startNs)))
            writer.signals?.setClock(true, time.offsetMinutes)
        } catch (error: Throwable) {
            writer.append("psftp_set_local_time_result", psftpResult(operationId, false, null, byteArrayOf(), safe(error), elapsedMs(startNs)))
            throw error
        }
    }

    private suspend fun bestEffortStop(connection: GattConnection, control: BluetoothGattCharacteristic, command: ByteArray, reason: String) {
        recoverableResult {
            connection.write(control, command, reason)
            connection.awaitNotification(H10Protocol.PMD_CONTROL, CONTROL_TIMEOUT_MS) { H10Protocol.isMatchingControlResponse(it, command) }
        }.onFailure { writer.event("pmd_pre_start_stop_failed", "warn", safe(it), RecordJson.fields(mapOf("reason" to reason))) }
    }

    private suspend fun startStream(connection: GattConnection, control: BluetoothGattCharacteristic, command: ByteArray, reason: String) {
        connection.write(control, command, reason)
        val response = connection.awaitNotification(H10Protocol.PMD_CONTROL, CONTROL_TIMEOUT_MS) { H10Protocol.isMatchingControlResponse(it, command) }
        check(H10Protocol.controlSucceeded(response)) { "$reason rejected: ${response.joinToString("") { "%02x".format(it) }}" }
    }

    private suspend fun stream(connection: GattConnection) {
        var lastHealthMs = 0L
        var lastBatteryMs = System.currentTimeMillis()
        var lastHrWatchdog = 0L
        val streamStartedMs = System.currentTimeMillis()
        var clockAnchorWallMs = System.currentTimeMillis()
        var clockAnchorElapsedMs = android.os.SystemClock.elapsedRealtime()
        while (currentCoroutineContext().isActive && settings.collectionEnabled) {
            if (connection.disconnected.isCompleted) throw IOException(connection.disconnected.await())
            val nowMs = System.currentTimeMillis()
            if (nowMs - lastHealthMs >= 1_000) {
                recoverableResult { connection.readRssi() }
                val graph = (context.applicationContext as com.vineyardfinance.sensors.SensorsApplication).graph
                val backlog = graph.queue.backlog()
                val uploadVitals = graph.uploader.vitals.snapshot()
                val freeBytes = context.filesDir.usableSpace
                val elapsedNow = android.os.SystemClock.elapsedRealtime()
                val wallDelta = nowMs - clockAnchorWallMs
                val elapsedDelta = elapsedNow - clockAnchorElapsedMs
                val clockStepMs = wallDelta - elapsedDelta
                writer.event("collector_health", "info", "streaming", RecordJson.fields(mapOf(
                    "backlog_records" to backlog.records,
                    "local_free_bytes" to freeBytes,
                    "phone_battery_percent" to batteryManager.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY),
                    "power_save_mode" to powerManager.isPowerSaveMode,
                    "device_interactive" to powerManager.isInteractive,
                    "ecg_age_ms" to ageMs(CollectorStatus.updates.value.ecgUnixNs),
                    "acc_age_ms" to ageMs(CollectorStatus.updates.value.accUnixNs),
                    "hr_age_ms" to ageMs(CollectorStatus.updates.value.hrUnixNs),
                    "battery_optimization_exempt" to powerManager.isIgnoringBatteryOptimizations(context.packageName),
                    "battery_charging" to batteryManager.isCharging,
                    "app_standby_bucket" to appStandbyBucket(),
                    "data_saver_status" to dataSaverStatus(),
                    "upload_iteration_age_ms" to UploadLoopVitals.ageMs(elapsedNow, uploadVitals.iterationAtMs),
                    "upload_attempt_age_ms" to UploadLoopVitals.ageMs(elapsedNow, uploadVitals.attemptStartedAtMs),
                    "upload_ack_age_ms" to UploadLoopVitals.ageMs(elapsedNow, uploadVitals.lastAckAtMs),
                    "upload_backoff_ms" to uploadVitals.backoffMs,
                    "upload_waiting_reconnect" to uploadVitals.waitingForReconnect,
                    "upload_consecutive_failures" to uploadVitals.consecutiveFailures,
                )))
                if (kotlin.math.abs(clockStepMs) > CLOCK_STEP_THRESHOLD_MS) {
                    writer.event("phone_clock_step_detected", "warn", "wall and elapsed clocks diverged", RecordJson.fields(mapOf("clock_step_ms" to clockStepMs)))
                    clockAnchorWallMs = nowMs
                    clockAnchorElapsedMs = elapsedNow
                }
                requireArchiveCapacity(freeBytes) {
                    writer.event("local_storage_critical", "critical", "insufficient durable queue storage", RecordJson.fields(mapOf("local_free_bytes" to freeBytes)))
                }
                lastHealthMs = nowMs
            }
            if (nowMs - lastBatteryMs >= 60_000) { readBattery(connection); lastBatteryMs = nowMs }
            val state = CollectorStatus.updates.value
            if (nowMs - streamStartedMs > WATCHDOG_GRACE_MS && ageMs(state.ecgUnixNs) > WATCHDOG_MS) {
                writer.event("ecg_watchdog_timeout", "error", "ECG stale >2s")
                throw IOException("ECG stream watchdog timeout")
            }
            if (nowMs - streamStartedMs > WATCHDOG_GRACE_MS && ageMs(state.accUnixNs) > WATCHDOG_MS) {
                writer.event("acc_watchdog_timeout", "error", "ACC stale >2s")
                throw IOException("ACC stream watchdog timeout")
            }
            if (ageMs(state.hrUnixNs) > WATCHDOG_MS && nowMs - lastHrWatchdog > WATCHDOG_MS) {
                lastHrWatchdog = nowMs; writer.event("hr_watchdog_timeout", "warn", "HR stale >2s")
            }
            delay(200)
        }
    }

    private suspend fun readBattery(connection: GattConnection) {
        val characteristic = connection.characteristic(H10Protocol.BATTERY_LEVEL) ?: return
        val payload = connection.read(characteristic, "battery_level")
        payload.firstOrNull()?.let { byte -> CollectorStatus.update { it.copy(batteryPercent = byte.toInt() and 0xff) } }
    }

    private fun required(connection: GattConnection, uuid: UUID): BluetoothGattCharacteristic =
        connection.characteristic(uuid) ?: throw IOException("required characteristic missing: $uuid")

    private fun psftpResult(id: String, success: Boolean, status: Int?, response: ByteArray, error: String?, elapsed: Long) = RecordJson.body {
        addProperty("operation_id", id); addProperty("success", success)
        if (status == null) add("status_code", JsonNull.INSTANCE) else addProperty("status_code", status)
        addProperty("response_payload_b64", RecordJson.payload(response)); nullable("error", error); addProperty("elapsed_ms", elapsed)
    }

    private fun ageMs(unixNs: Long?): Long = unixNs?.let { (System.currentTimeMillis() - it / 1_000_000L).coerceAtLeast(0) } ?: Long.MAX_VALUE

    // Power-state facts for the overnight-stall investigation; failures report -1 rather
    // than suppressing the health event.
    private fun appStandbyBucket(): Int = runCatching {
        context.getSystemService(UsageStatsManager::class.java).appStandbyBucket
    }.getOrDefault(-1)

    private fun dataSaverStatus(): Int = runCatching {
        context.getSystemService(ConnectivityManager::class.java).restrictBackgroundStatus
    }.getOrDefault(-1)
    private fun throwFromCallback(error: Throwable) { CollectorStatus.update { it.copy(lastError = safe(error)) } }

    internal class ArchiveCriticalStorageFailure(
        val freeBytes: Long,
        val minimumFreeBytes: Long,
    ) : IOException(
        "local storage below ${minimumFreeBytes / (1024 * 1024)} MiB safety threshold " +
            "($freeBytes bytes free)",
    )

    companion object {
        const val SCAN_TIMEOUT_MS = 10_000L
        const val CONTROL_TIMEOUT_MS = 3_000L
        const val PSFTP_TIMEOUT_MS = 8_000L
        const val WATCHDOG_MS = 2_000L
        const val WATCHDOG_GRACE_MS = 3_000L
        const val CLOCK_STEP_THRESHOLD_MS = 1_000L
        const val MIN_FREE_BYTES = 256L * 1024L * 1024L
        const val CONNECTION_CLEANUP_TIMEOUT_MS = 4_000L
        const val SESSION_END_TIMEOUT_MS = 3_000L
        private fun elapsedMs(startNs: Long) = ((System.nanoTime() - startNs) / 1_000_000L).coerceAtLeast(0)
        private fun safe(error: Throwable) = (error.message ?: error.javaClass.simpleName).take(240)

        internal suspend fun requireArchiveCapacity(
            freeBytes: Long,
            minimumFreeBytes: Long = MIN_FREE_BYTES,
            recordCriticalEvent: suspend () -> Unit,
        ) {
            if (freeBytes >= minimumFreeBytes) return
            val failure = ArchiveCriticalStorageFailure(freeBytes, minimumFreeBytes)
            try {
                recordCriticalEvent()
            } catch (error: Throwable) {
                // Never turn service cancellation into a failure latch. If recording the critical
                // event fails while the collector is still active, retain that evidence on the
                // archive-critical exception and stop collection anyway.
                currentCoroutineContext().ensureActive()
                failure.addSuppressed(error)
            }
            throw failure
        }

        internal suspend fun handleConnectionCycleFailure(
            error: Throwable,
            onRecoverable: suspend (Throwable) -> Unit,
        ) {
            // TimeoutCancellationException cancels only the bounded operation's child scope. The
            // collector's parent remains active and must reconnect. A real service-job cancellation
            // makes ensureActive throw and prevents an accidental reconnect during shutdown.
            currentCoroutineContext().ensureActive()
            // A storage safety stop is archive-critical rather than a BLE connection failure. Let
            // it escape to CollectorService so the durable failure latch and persistent
            // action-required foreground notification remain active until deliberate recovery.
            if (error is ArchiveCriticalStorageFailure) throw error
            onRecoverable(error)
        }

        internal suspend fun <T> recoverableResult(block: suspend () -> T): Result<T> = try {
            Result.success(block())
        } catch (error: Throwable) {
            // Best-effort operation failures may be ignored, but a true service-job cancellation
            // must never be converted into success. Child withTimeout failures leave this active.
            currentCoroutineContext().ensureActive()
            Result.failure(error)
        }
    }
}
