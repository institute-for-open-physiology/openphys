package com.vineyardfinance.sensors.collector

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.bluetooth.BluetoothStatusCodes
import android.content.Context
import android.os.Build
import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.data.RecordJson
import com.vineyardfinance.sensors.data.RecordJson.nullable
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.io.Closeable
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

@SuppressLint("MissingPermission")
class GattConnection(
    private val appContext: Context,
    private val writer: RawRecordWriter,
    private val fatal: (Throwable) -> Unit,
) : Closeable {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val callbackEvents = BoundedIngress<CallbackEvent>(CALLBACK_EVENT_CAPACITY)
    private val commandResponses = CommandResponseRouter(
        setOf(H10Protocol.PMD_CONTROL, H10Protocol.PSFTP_MTU),
        COMMAND_RESPONSE_CAPACITY,
    )
    private val operationMutex = Mutex()
    private val connected = CompletableDeferred<Unit>()
    private val servicesDiscovered = CompletableDeferred<Unit>()
    private val mtuChanged = CompletableDeferred<MtuResult>()
    val disconnected = CompletableDeferred<String>()
    private val closed = AtomicBoolean(false)
    private val captureFailed = AtomicBoolean(false)
    @Volatile private var pending: Pending? = null
    @Volatile private var gatt: BluetoothGatt? = null

    init {
        scope.launch {
            try {
                while (true) handle(callbackEvents.receive())
            } catch (error: Throwable) {
                failCapture(error)
            }
        }
    }

    suspend fun connect(device: BluetoothDevice) {
        val attempt = UUID.randomUUID().toString()
        writer.peripheralId = device.address.lowercase()
        writer.append("connect_attempt", RecordJson.body {
            addProperty("attempt_id", attempt)
            addProperty("peripheral_id", writer.peripheralId)
            nullable("local_name", runCatching { device.name }.getOrNull())
            add("rssi_dbm", JsonNull.INSTANCE)
        })
        val start = System.nanoTime()
        gatt = device.connectGatt(appContext, false, callback, BluetoothDevice.TRANSPORT_LE)
            ?: throw IOException("connectGatt returned null")
        try {
            // Android's connection callback is bounded so a lost callback cannot stall collection.
            withTimeout(CONNECT_TIMEOUT_MS) { connected.await() }
            gatt?.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH)
            writer.append("connect_result", RecordJson.body {
                addProperty("attempt_id", attempt); addProperty("connected", true); addProperty("elapsed_ms", elapsedMs(start))
            })
        } catch (error: Throwable) {
            writer.append("connect_error", RecordJson.body {
                addProperty("attempt_id", attempt); addProperty("stage", "connect"); addProperty("error", safe(error)); addProperty("elapsed_ms", elapsedMs(start))
            })
            throw error
        }
    }

    suspend fun discoverServices() {
        val current = requireGatt()
        check(current.discoverServices()) { "discoverServices was rejected" }
        // Service discovery is bounded independently of connection establishment.
        withTimeout(DISCOVER_TIMEOUT_MS) { servicesDiscovered.await() }
    }

    suspend fun negotiateMtu(requestedMtu: Int = REQUESTED_MTU): Int {
        require(requestedMtu >= MIN_REQUIRED_MTU)
        val start = System.nanoTime()
        writer.event(
            "gatt_mtu_request",
            "info",
            "requesting larger ATT MTU before PMD streaming",
            RecordJson.fields(mapOf("requested_mtu" to requestedMtu, "minimum_required_mtu" to MIN_REQUIRED_MTU)),
        )
        try {
            check(requireGatt().requestMtu(requestedMtu)) { "requestMtu was rejected" }
            val result = withTimeout(MTU_TIMEOUT_MS) { mtuChanged.await() }
            val negotiatedMtu = validateMtuResult(result.mtu, result.status)
            writer.event(
                "gatt_mtu_result",
                "info",
                "ATT MTU negotiated",
                RecordJson.fields(
                    mapOf(
                        "requested_mtu" to requestedMtu,
                        "negotiated_mtu" to result.mtu,
                        "status" to result.status,
                        "elapsed_ms" to elapsedMs(start),
                    ),
                ),
            )
            return negotiatedMtu
        } catch (error: Throwable) {
            writer.event(
                "gatt_mtu_error",
                "error",
                safe(error),
                RecordJson.fields(mapOf("requested_mtu" to requestedMtu, "elapsed_ms" to elapsedMs(start))),
            )
            throw error
        }
    }

    suspend fun ensureBonded(): Boolean {
        val device = requireGatt().device
        if (device.bondState == BluetoothDevice.BOND_BONDED) return true
        CollectorStatus.update {
            it.transitionTo(
                state = "Pairing required",
                detail = "Confirm the Polar H10 pairing prompt to enable PMD and clock setting",
            )
        }
        writer.event(
            "bond_attempt",
            "warn",
            "requesting one-time encrypted H10 bond",
            RecordJson.fields(mapOf("peripheral_id" to writer.peripheralId, "timeout_ms" to BOND_TIMEOUT_MS)),
        )
        check(device.createBond()) { "Android rejected H10 createBond request" }
        try {
            withTimeout(BOND_TIMEOUT_MS) {
                var sawBonding = false
                while (device.bondState != BluetoothDevice.BOND_BONDED) {
                    if (device.bondState == BluetoothDevice.BOND_BONDING) sawBonding = true
                    check(!(sawBonding && device.bondState == BluetoothDevice.BOND_NONE)) {
                        "H10 bond was rejected or cancelled"
                    }
                    delay(BOND_POLL_MS)
                }
            }
            writer.event("bond_result", "info", "H10 bond established")
            return true
        } catch (error: Throwable) {
            writer.event("bond_error", "error", safe(error))
            throw error
        }
    }

    fun characteristic(uuid: UUID): BluetoothGattCharacteristic? =
        requireGatt().services.asSequence().flatMap { it.characteristics.asSequence() }.firstOrNull { it.uuid == uuid }

    fun isBonded(): Boolean = requireGatt().device.bondState == BluetoothDevice.BOND_BONDED

    fun serviceSummary(deviceInfo: Map<String, String?>): JsonObject = RecordJson.body {
        addProperty("peripheral_id", writer.peripheralId)
        val serviceValues = JsonArray()
        requireGatt().services.sortedBy { it.uuid.toString() }.forEach { service ->
            serviceValues.add(JsonObject().apply {
                addProperty("service_uuid", service.uuid.toString().lowercase())
                add("characteristics", JsonArray().apply {
                    service.characteristics.sortedBy { it.uuid.toString() }.forEach { characteristic ->
                        add(JsonObject().apply {
                            addProperty("characteristic_uuid", characteristic.uuid.toString().lowercase())
                            add("properties", JsonArray().apply { properties(characteristic.properties).forEach(::add) })
                        })
                    }
                })
            })
        }
        addProperty("service_count", serviceValues.size())
        add("services", serviceValues)
        add("device_info", JsonObject().apply {
            listOf("manufacturer", "model", "serial", "hardware_revision", "firmware_revision", "software_revision").forEach { nullable(it, deviceInfo[it]) }
        })
    }

    suspend fun read(characteristic: BluetoothGattCharacteristic, reason: String, timeoutMs: Long = OPERATION_TIMEOUT_MS): ByteArray = operationMutex.withLock {
        val operation = Pending("read", characteristic.uuid, reason)
        writer.append("gatt_read_request", RecordJson.body {
            addProperty("operation_id", operation.id); addProperty("characteristic_uuid", characteristic.uuid.text()); addProperty("reason", reason)
        })
        pending = operation
        val accepted = requireGatt().readCharacteristic(characteristic)
        if (!accepted) operation.result.completeExceptionally(IOException("readCharacteristic rejected"))
        awaitOperation(operation, timeoutMs, "gatt_read_error")
    }

    suspend fun write(characteristic: BluetoothGattCharacteristic, payload: ByteArray, reason: String, timeoutMs: Long = OPERATION_TIMEOUT_MS): ByteArray = operationMutex.withLock {
        val operation = Pending("write", characteristic.uuid, reason)
        writer.append("gatt_write_request", RecordJson.body {
            addProperty("operation_id", operation.id)
            addProperty("characteristic_uuid", characteristic.uuid.text())
            addProperty("write_type", "with_response")
            addProperty("reason", reason)
            addProperty("payload_b64", RecordJson.payload(payload))
            addProperty("payload_len", payload.size)
        })
        pending = operation
        val accepted = if (Build.VERSION.SDK_INT >= 33) {
            requireGatt().writeCharacteristic(characteristic, payload, BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT) == BluetoothStatusCodes.SUCCESS
        } else {
            @Suppress("DEPRECATION")
            characteristic.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
            @Suppress("DEPRECATION")
            characteristic.value = payload
            @Suppress("DEPRECATION")
            requireGatt().writeCharacteristic(characteristic)
        }
        if (!accepted) operation.result.completeExceptionally(IOException("writeCharacteristic rejected"))
        awaitOperation(operation, timeoutMs, "gatt_write_error")
    }

    suspend fun subscribe(characteristic: BluetoothGattCharacteristic, reason: String): ByteArray = operationMutex.withLock {
        val operation = Pending("subscription", characteristic.uuid, reason)
        val indicate = characteristic.properties and BluetoothGattCharacteristic.PROPERTY_INDICATE != 0 &&
            characteristic.properties and BluetoothGattCharacteristic.PROPERTY_NOTIFY == 0
        writer.append("subscription_attempt", RecordJson.body {
            addProperty("operation_id", operation.id); addProperty("characteristic_uuid", characteristic.uuid.text())
            addProperty("notify_kind", if (indicate) "indicate" else "notify"); addProperty("reason", reason)
        })
        check(requireGatt().setCharacteristicNotification(characteristic, true)) { "setCharacteristicNotification rejected" }
        val descriptor = characteristic.getDescriptor(H10Protocol.CCCD) ?: throw IOException("CCCD missing for ${characteristic.uuid}")
        pending = operation
        val value = if (indicate) BluetoothGattDescriptor.ENABLE_INDICATION_VALUE else BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
        val accepted = if (Build.VERSION.SDK_INT >= 33) {
            requireGatt().writeDescriptor(descriptor, value) == BluetoothStatusCodes.SUCCESS
        } else {
            @Suppress("DEPRECATION") descriptor.value = value
            @Suppress("DEPRECATION") requireGatt().writeDescriptor(descriptor)
        }
        if (!accepted) operation.result.completeExceptionally(IOException("writeDescriptor rejected"))
        awaitOperation(operation, OPERATION_TIMEOUT_MS, "subscription_error")
    }

    suspend fun readRssi(): Int = operationMutex.withLock {
        val operation = Pending("rssi", UUID(0, 0), "android_read_remote_rssi")
        pending = operation
        if (!requireGatt().readRemoteRssi()) operation.result.completeExceptionally(IOException("readRemoteRssi rejected"))
        try {
            // RSSI health polling gets a short bound and never blocks notification handling.
            withTimeout(RSSI_TIMEOUT_MS) { operation.result.await() }.firstOrNull()?.toInt() ?: error("missing RSSI")
        } catch (error: Throwable) {
            writer.append("rssi_read_error", RecordJson.body {
                addProperty("peripheral_id", writer.peripheralId); addProperty("method", "android_read_remote_rssi")
                addProperty("error", safe(error)); addProperty("elapsed_ms", elapsedMs(operation.startedNs))
            })
            throw error
        } finally { if (pending === operation) pending = null }
    }

    suspend fun awaitNotification(uuid: UUID, timeoutMs: Long, predicate: (ByteArray) -> Boolean = { true }): ByteArray = withTimeout(timeoutMs) {
        // This notification receive is bounded by the caller's absolute operation timeout.
        commandResponses.receiveMatching(uuid, predicate)
    }

    suspend fun disconnect(reason: String) {
        val start = System.nanoTime()
        writer.append("disconnect_attempt", RecordJson.body { addProperty("reason", reason); addProperty("peripheral_id", writer.peripheralId) })
        requireGatt().disconnect()
        runCatching {
            // Cleanup is explicitly bounded; close() follows even if the platform omits a callback.
            withTimeout(CLEANUP_TIMEOUT_MS) { disconnected.await() }
        }.onSuccess {
            writer.append("disconnect_result", RecordJson.body { addProperty("reason", reason); addProperty("elapsed_ms", elapsedMs(start)) })
        }.onFailure {
            writer.append("disconnect_error", RecordJson.body { addProperty("reason", reason); addProperty("error", safe(it)); addProperty("elapsed_ms", elapsedMs(start)) })
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        runCatching { gatt?.close() }
        callbackEvents.close()
        commandResponses.close()
        scope.cancel()
    }

    private suspend fun awaitOperation(operation: Pending, timeoutMs: Long, errorKind: String): ByteArray {
        return try {
            // Every read, write, and CCCD operation is serialized and bounded here.
            withTimeout(timeoutMs) { operation.result.await() }
        } catch (error: Throwable) {
            val body = RecordJson.body {
                addProperty("operation_id", operation.id); addProperty("characteristic_uuid", operation.uuid.text())
                if (errorKind == "gatt_read_error") addProperty("reason", operation.reason)
                addProperty("error", safe(error)); addProperty("elapsed_ms", elapsedMs(operation.startedNs))
            }
            writer.append(errorKind, body)
            throw error
        } finally { if (pending === operation) pending = null }
    }

    private suspend fun handle(event: CallbackEvent) {
        when (event) {
            is CallbackEvent.Connection -> {
                writer.appendCaptured("central_event", RecordJson.body {
                    addProperty("event_name", if (event.state == BluetoothProfile.STATE_CONNECTED) "device_connected" else "device_disconnected")
                    addProperty("peripheral_id", writer.peripheralId); addProperty("detail", "android_status=${event.status} state=${event.state}")
                    add("rssi_dbm", JsonNull.INSTANCE); add("service_uuids", JsonArray()); addProperty("raw_debug", "BluetoothGattCallback.onConnectionStateChange")
                }, event.unixNs, event.elapsedNs)
                if (event.status == BluetoothGatt.GATT_SUCCESS && event.state == BluetoothProfile.STATE_CONNECTED) connected.complete(Unit)
                else {
                    val error = IOException("GATT disconnected status=${event.status} state=${event.state}")
                    if (!connected.isCompleted) connected.completeExceptionally(error)
                    if (!disconnected.isCompleted) disconnected.complete(error.message.orEmpty())
                    pending?.result?.completeExceptionally(error)
                }
            }
            is CallbackEvent.Services -> if (event.status == BluetoothGatt.GATT_SUCCESS) servicesDiscovered.complete(Unit)
                else servicesDiscovered.completeExceptionally(IOException("service discovery status=${event.status}"))
            is CallbackEvent.Mtu -> mtuChanged.complete(MtuResult(event.mtu, event.status))
            is CallbackEvent.CharacteristicChanged -> {
                val description = description(event.uuid)
                writer.appendCaptured("gatt_notification", RecordJson.body {
                    addProperty("characteristic_uuid", event.uuid.text()); addProperty("description", description)
                    addProperty("payload_b64", RecordJson.payload(event.value)); addProperty("payload_len", event.value.size)
                }, event.unixNs, event.elapsedNs)
                writer.signals?.receive(event.uuid, event.value, event.unixNs / 1e9)
                when {
                    event.uuid == H10Protocol.PMD_DATA && event.value.firstOrNull() == 0.toByte() -> CollectorStatus.update { it.copy(ecgUnixNs = event.unixNs) }
                    event.uuid == H10Protocol.PMD_DATA && event.value.firstOrNull() == 2.toByte() -> CollectorStatus.update { it.copy(accUnixNs = event.unixNs) }
                    event.uuid == H10Protocol.HR_MEASUREMENT -> CollectorStatus.update { it.copy(hrUnixNs = event.unixNs) }
                }
                val route = commandResponses.route(
                    RoutedGattNotification(event.uuid, event.value, event.unixNs, event.elapsedNs),
                )
                if (route == NotificationRouteResult.FULL || route == NotificationRouteResult.CLOSED) {
                    failCapture(IOException("BLE command-response buffer $route; disconnecting before capture can become silent"))
                }
            }
            is CallbackEvent.CharacteristicRead -> {
                val operation = pending
                if (operation?.kind != "read" || operation.uuid != event.uuid) return
                if (event.status == BluetoothGatt.GATT_SUCCESS) {
                    writer.appendCaptured("gatt_read_response", RecordJson.body {
                        addProperty("operation_id", operation.id); addProperty("characteristic_uuid", event.uuid.text()); addProperty("reason", operation.reason)
                        addProperty("payload_b64", RecordJson.payload(event.value)); addProperty("payload_len", event.value.size)
                    }, event.unixNs, event.elapsedNs)
                    operation.result.complete(event.value)
                } else operation.result.completeExceptionally(IOException("GATT read status=${event.status}"))
            }
            is CallbackEvent.CharacteristicWrite -> completeSimple(event.uuid, "write", event.status, event.unixNs, event.elapsedNs, "gatt_write_result")
            is CallbackEvent.DescriptorWrite -> completeSimple(event.uuid, "subscription", event.status, event.unixNs, event.elapsedNs, "subscription_result")
            is CallbackEvent.Rssi -> {
                val operation = pending
                if (operation?.kind != "rssi") return
                if (event.status == BluetoothGatt.GATT_SUCCESS) {
                    writer.appendCaptured("rssi_sample", RecordJson.body {
                        addProperty("peripheral_id", writer.peripheralId); addProperty("rssi_dbm", event.rssi)
                        addProperty("method", "unknown"); addProperty("age_ms", 0)
                    }, event.unixNs, event.elapsedNs)
                    CollectorStatus.update { it.copy(rssiDbm = event.rssi) }
                    writer.signals?.receiveRSSI(event.rssi, event.unixNs / 1e9)
                    operation.result.complete(byteArrayOf(event.rssi.toByte()))
                } else operation.result.completeExceptionally(IOException("RSSI status=${event.status}"))
            }
        }
    }

    private suspend fun completeSimple(uuid: UUID, expectedKind: String, status: Int, unixNs: Long, elapsedNs: Long, resultKind: String) {
        val operation = pending
        if (operation?.kind != expectedKind || operation.uuid != uuid) return
        if (status == BluetoothGatt.GATT_SUCCESS) {
            writer.appendCaptured(resultKind, RecordJson.body {
                addProperty("operation_id", operation.id); addProperty("characteristic_uuid", uuid.text()); addProperty("elapsed_ms", elapsedMs(operation.startedNs))
            }, unixNs, elapsedNs)
            operation.result.complete(byteArrayOf())
        } else operation.result.completeExceptionally(IOException("GATT $expectedKind status=$status"))
    }

    private fun enqueue(event: CallbackEvent) {
        if (closed.get() || captureFailed.get()) return
        when (callbackEvents.tryEnqueue(event)) {
            EnqueueResult.ENQUEUED -> Unit
            EnqueueResult.FULL -> failCapture(IOException("BLE callback buffer overflow; disconnecting before capture can become silent"))
            EnqueueResult.CLOSED -> failCapture(IOException("BLE callback buffer closed; capture cannot continue safely"))
        }
    }

    private fun failCapture(error: Throwable) {
        if (closed.get() || !captureFailed.compareAndSet(false, true)) return
        if (!disconnected.isCompleted) disconnected.complete("fatal BLE capture failure: ${safe(error)}")
        pending?.result?.completeExceptionally(error)
        fatal(error)
        runCatching { gatt?.disconnect() }
    }

    private val callback = object : BluetoothGattCallback() {
        override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) = enqueue(CallbackEvent.Connection(status, newState, times()))
        override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) = enqueue(CallbackEvent.Services(status, times()))
        override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) =
            enqueue(CallbackEvent.Mtu(mtu, status, times()))
        override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, value: ByteArray) =
            enqueue(CallbackEvent.CharacteristicChanged(characteristic.uuid, value.copyOf(), times()))
        @Deprecated("API 33 callback")
        override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) =
            enqueue(CallbackEvent.CharacteristicChanged(characteristic.uuid, characteristic.value?.copyOf() ?: byteArrayOf(), times()))
        override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, value: ByteArray, status: Int) =
            enqueue(CallbackEvent.CharacteristicRead(characteristic.uuid, value.copyOf(), status, times()))
        @Deprecated("API 33 callback")
        override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) =
            enqueue(CallbackEvent.CharacteristicRead(characteristic.uuid, characteristic.value?.copyOf() ?: byteArrayOf(), status, times()))
        override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) =
            enqueue(CallbackEvent.CharacteristicWrite(characteristic.uuid, status, times()))
        override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) =
            enqueue(CallbackEvent.DescriptorWrite(descriptor.characteristic.uuid, status, times()))
        override fun onReadRemoteRssi(gatt: BluetoothGatt, rssi: Int, status: Int) = enqueue(CallbackEvent.Rssi(rssi, status, times()))
    }

    private fun requireGatt(): BluetoothGatt = gatt ?: error("GATT not connected")

    private data class Pending(
        val kind: String,
        val uuid: UUID,
        val reason: String,
        val id: String = UUID.randomUUID().toString(),
        val startedNs: Long = System.nanoTime(),
        val result: CompletableDeferred<ByteArray> = CompletableDeferred(),
    )
    private data class Times(val unixNs: Long, val elapsedNs: Long)
    private sealed class CallbackEvent(open val captured: Times) {
        val unixNs get() = captured.unixNs
        val elapsedNs get() = captured.elapsedNs
        data class Connection(val status: Int, val state: Int, override val captured: Times) : CallbackEvent(captured)
        data class Services(val status: Int, override val captured: Times) : CallbackEvent(captured)
        data class Mtu(val mtu: Int, val status: Int, override val captured: Times) : CallbackEvent(captured)
        data class CharacteristicChanged(val uuid: UUID, val value: ByteArray, override val captured: Times) : CallbackEvent(captured)
        data class CharacteristicRead(val uuid: UUID, val value: ByteArray, val status: Int, override val captured: Times) : CallbackEvent(captured)
        data class CharacteristicWrite(val uuid: UUID, val status: Int, override val captured: Times) : CallbackEvent(captured)
        data class DescriptorWrite(val uuid: UUID, val status: Int, override val captured: Times) : CallbackEvent(captured)
        data class Rssi(val rssi: Int, val status: Int, override val captured: Times) : CallbackEvent(captured)
    }

    companion object {
        const val CONNECT_TIMEOUT_MS = 8_000L
        const val DISCOVER_TIMEOUT_MS = 8_000L
        const val MTU_TIMEOUT_MS = 8_000L
        const val BOND_TIMEOUT_MS = 60_000L
        const val BOND_POLL_MS = 200L
        const val REQUESTED_MTU = 247
        const val MIN_REQUIRED_MTU = 232
        const val OPERATION_TIMEOUT_MS = 3_000L
        const val RSSI_TIMEOUT_MS = 500L
        const val CLEANUP_TIMEOUT_MS = 2_000L
        internal const val CALLBACK_EVENT_CAPACITY = 1_024
        internal const val COMMAND_RESPONSE_CAPACITY = 32
        private fun UUID.text() = toString().lowercase()
        private fun times() = RawRecordWriter.capturedTimes().let { Times(it.first, it.second) }
        private fun elapsedMs(startNs: Long) = ((System.nanoTime() - startNs) / 1_000_000L).coerceAtLeast(0)
        private fun safe(error: Throwable) = (error.message ?: error.javaClass.simpleName).take(240)
        private fun description(uuid: UUID) = when (uuid) {
            H10Protocol.PMD_DATA -> "pmd_data"
            H10Protocol.PMD_CONTROL -> "pmd_control"
            H10Protocol.HR_MEASUREMENT -> "heart_rate_measurement"
            H10Protocol.BATTERY_LEVEL -> "battery_level"
            H10Protocol.PSFTP_MTU -> "psftp_mtu"
            else -> "unknown"
        }
        private fun properties(value: Int): List<String> = buildList {
            if (value and BluetoothGattCharacteristic.PROPERTY_READ != 0) add("read")
            if (value and BluetoothGattCharacteristic.PROPERTY_WRITE != 0) add("write")
            if (value and BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE != 0) add("write_without_response")
            if (value and BluetoothGattCharacteristic.PROPERTY_NOTIFY != 0) add("notify")
            if (value and BluetoothGattCharacteristic.PROPERTY_INDICATE != 0) add("indicate")
        }

        internal fun validateMtuResult(mtu: Int, status: Int): Int {
            check(status == BluetoothGatt.GATT_SUCCESS) { "MTU negotiation status=$status" }
            check(mtu >= MIN_REQUIRED_MTU) {
                "negotiated MTU $mtu is below required $MIN_REQUIRED_MTU"
            }
            return mtu
        }
    }

    internal data class MtuResult(val mtu: Int, val status: Int)
}
