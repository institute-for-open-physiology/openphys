package com.vineyardfinance.sensors.seca

import android.annotation.SuppressLint
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothSocket
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.os.SystemClock
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.R
import com.vineyardfinance.sensors.SensorsApplication
import com.vineyardfinance.sensors.collector.RawRecordWriter
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.location.LocationCollector
import com.vineyardfinance.sensors.network.UploadLoopSupervisor
import com.vineyardfinance.sensors.network.UploadWorker
import com.vineyardfinance.sensors.setup.RequiredPermissions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** Android Bluetooth/socket boundary. Every blocking radio operation is bounded by socket closure. */
class SecaService : Service() {
    private val graph get() = (application as SensorsApplication).graph
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var recording: Job? = null
    private var uploading: Job? = null
    private var locationUploading: Job? = null
    private var appliedForegroundTypes = -1
    private var latestStartId = 0
    @Volatile private var destroyed = false
    @Volatile private var socket: BluetoothSocket? = null
    private var wakeLock: PowerManager.WakeLock? = null
    private val writeLock = Mutex()
    @Volatile private var activeCapture: Capture? = null

    override fun onBind(intent: Intent?): IBinder? = null
    override fun onCreate() {
        super.onCreate()
        getSystemService(NotificationManager::class.java).createNotificationChannel(
            NotificationChannel(CHANNEL, "seca ECG recording", NotificationManager.IMPORTANCE_LOW),
        )
        refreshForegroundTypes(allowLocation = false)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        latestStartId = startId
        if (intent?.action == STOP) {
            try { graph.secaSettings.stop() } catch (error: Exception) { report(error) }
            val stoppingSocket = socket
            val stoppingCapture = activeCapture
            scope.launch {
                try { stoppingSocket?.let { send(it, "mode=standby", stoppingCapture) } }
                catch (error: Exception) { Log.w(TAG, "seca standby failed during stop", error) }
                finally {
                    try { stoppingSocket?.close() } catch (error: IOException) { Log.w(TAG, "seca stop socket cleanup", error) }
                    withContext(Dispatchers.Main) { if (recording == null && !graph.secaSettings.desired) stopSelfResult(startId) }
                }
            }
            return START_NOT_STICKY
        }
        if (!graph.secaSettings.desired) { stopSelfResult(startId); return START_NOT_STICKY }
        if (recording?.isActive != true) {
            recording = scope.launch { recordLoop() }
        }
        if (uploading?.isActive != true) {
            uploading = scope.launch {
                while (isActive) {
                    try { graph.secaUploader.drain() }
                    catch (cancelled: CancellationException) { throw cancelled }
                    catch (error: Exception) { SecaStatus.update { it.copy(uploadError = error.message ?: "Upload failed; retained on phone") } }
                    delay(1000)
                }
            }
        }
        if (locationUploading?.isActive != true) {
            locationUploading = scope.launch {
                UploadLoopSupervisor(
                    vitals = graph.uploader.vitals,
                    queuedRecords = { graph.queue.backlog().records },
                    clock = SystemClock::elapsedRealtime,
                    onStall = { SecaStatus.update { it.copy(locationUpload = "GPS upload stalled; retrying") } },
                    onLoopFailure = { detail ->
                        Log.e(TAG, "GPS upload: $detail")
                        SecaStatus.update { it.copy(locationUpload = detail) }
                    },
                ).supervise(active = { graph.secaSettings.desired }) {
                    graph.uploader.retryLoop(active = { graph.secaSettings.desired }) { message ->
                        SecaStatus.update { it.copy(locationUpload = message) }
                    }
                }
            }
        }
        return START_STICKY
    }

    private suspend fun recordLoop() {
        SecaStatus.update { it.copy(running = true, error = null) }
        wakeLock = getSystemService(PowerManager::class.java).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "TrainingSensors:seca").apply { setReferenceCounted(false); acquire(10 * 60 * 1000L) }
        val renew = scope.launch {
            while (isActive) { delay(5 * 60 * 1000L); wakeLock?.acquire(10 * 60 * 1000L) }
        }
        try {
            while (scope.isActive && graph.secaSettings.desired) {
                try { connectAndRecord() }
                catch (cancelled: CancellationException) { throw cancelled }
                catch (error: ArchiveFailure) {
                    graph.secaSettings.stop()
                    report(error)
                    break // Never reconnect past a failure to preserve incoming bytes.
                }
                catch (error: Exception) {
                    if (graph.secaSettings.desired) report(error)
                }
                finally { closeSocket() }
                if (graph.secaSettings.desired) {
                    SecaStatus.update { it.copy(location = "GPS paused while seca reconnects") }
                    status("Disconnected; reconnecting in 3 seconds")
                    delay(3000)
                }
            }
        } finally {
            renew.cancel()
            closeSocket()
            wakeLock?.let { if (it.isHeld) it.release() }
            SecaUploadWorker.enqueue(this, freshAfterStop = true)
            UploadWorker.enqueue(this)
            withContext(NonCancellable + Dispatchers.Main) {
                recording = null
                if (!destroyed && graph.secaSettings.desired) {
                    // A newer Start can arrive while the old session is flushing. Its
                    // desired state wins, after all old socket/journal ownership ends.
                    recording = scope.launch { recordLoop() }
                } else {
                    SecaStatus.update { it.copy(running = false, state = "Stopped · saved packets continue uploading", location = "GPS stopped; saved fixes remain queued for upload") }
                    stopForeground(STOP_FOREGROUND_REMOVE)
                    stopSelfResult(latestStartId)
                }
            }
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun connectAndRecord() = coroutineScope {
        val address = graph.secaSettings.address ?: throw IOException("No seca selected")
        val rate = graph.secaSettings.rate
        val adapter = getSystemService(BluetoothManager::class.java)?.adapter ?: throw IOException("Bluetooth unavailable")
        if (!adapter.isEnabled) throw IOException("Turn Bluetooth on to record seca ECG")
        status("Connecting to $address")
        val connected = adapter.getRemoteDevice(address).createRfcommSocketToServiceRecord(SPP)
        socket = connected
        // Android's blocking connect/read/write have no timeout API. Closing their socket
        // from an independent coroutine unblocks the platform operation.
        val connectionDeadline = launch { delay(15_000); closeSocket() }
        try { connected.connect() } finally { connectionDeadline.cancel() }
        val capture = Capture(address, rate)
        activeCapture = capture
        // Service ownership keeps a stuck platform GPS cleanup from holding the
        // Bluetooth coroutineScope open. Explicit cancel/bounded join below; the
        // shared lease prevents new listeners until any old cleanup really ends.
        val location = scope.launch { superviseLocation(capture.sessionId) }
        val confirmation = CompletableDeferred<Unit>()
        val configuration = mutableMapOf<String, String>()
        val liveness = SecaLiveness(SystemClock.elapsedRealtime())
        var endReason = "connection_closed"
        val watchdogCause = AtomicReference<String?>(null)
        var keepalive: Job? = null
        val watchdog = launch {
            while (isActive) {
                delay(1000)
                Log.i("SecaIngress", capture.ingress.diagnostic(SystemClock.elapsedRealtimeNanos()))
                val failure = capture.ingress.recoveryTimeout(SystemClock.elapsedRealtimeNanos()) ?: capture.ingress.stalled(SystemClock.elapsedRealtimeNanos()) ?: liveness.failure(SystemClock.elapsedRealtime())
                if (failure != null) {
                    watchdogCause.set(failure)
                    Log.e(TAG, failure)
                    SecaStatus.update { it.copy(error = failure) }
                    closeSocket()
                    break
                }
            }
        }
        val archiveFailure = AtomicReference<Exception?>(null)
        val consumer = async {
            try {
                while (true) {
                    val entry = capture.ingress.next() ?: break
                    SecaTiming.measure("frames.retain") { entry.records.forEach { capture.append(it) } }
                    capture.ingress.complete(entry)
                    val received = entry.records.filter { it.direction == "rx" }
                    SecaStatus.update { it.copy(packets = it.packets + received.size, bytes = it.bytes + received.sumOf { record -> record.bytes.size }) }
                }
            } catch (error: Exception) {
                val failure = IOException("${error.message}; unsaved backlog upper bound: ${capture.ingress.diagnostic(SystemClock.elapsedRealtimeNanos())}", error)
                archiveFailure.set(failure)
                capture.freeze(failure)
                capture.ingress.abort(failure)
                confirmation.completeExceptionally(failure)
                closeSocket()
            }
        }
        val reader = async {
            try {
                val buffer = ByteArray(8192)
                while (isActive) {
                    val acquisition = capture.confirmed.get() // Conservative if run starts during this blocking read.
                    val count = SecaTiming.measure("bluetooth.read") { connected.inputStream.read(buffer) }
                    if (count < 0) throw IOException("seca Bluetooth disconnected")
                    if (count == 0) continue
                    liveness.bytes(SystemClock.elapsedRealtime())
                    val frames = capture.ingress.receive(buffer, count, acquisition)
                    capture.ingress.recoveryStats()?.let { stats ->
                        if (SecaStatus.updates.value.recovery != stats) {
                            SecaStatus.update { it.copy(recovery = stats) }
                            if (stats.episodes > 0) Log.w(TAG, "seca framing recovery: $stats")
                        }
                    }
                    for (frame in frames) {
                        liveness.frame(frame, SystemClock.elapsedRealtime())
                        if (BuildConfig.SECA_EXPERIMENTAL_LED_KEEPALIVE) capture.ledStatus.observe(frame, SystemClock.elapsedRealtime())
                        configuration.putAll(SecaProtocol.configuration(frame))
                        if (configuration["samplerate"]?.toIntOrNull() == rate && configuration["gain"]?.toIntOrNull() == 6 &&
                            configuration["openlead"]?.toIntOrNull() == 0 && configuration["compressed"]?.toIntOrNull() == 1 && configuration["mode"] == "standby") confirmation.complete(Unit)
                    }
                }
                null
            } catch (error: Exception) {
                confirmation.completeExceptionally(error)
                closeSocket()
                error
            } finally {
                capture.ingress.end()
                capture.ingress.recoveryStats()?.let { stats -> SecaStatus.update { it.copy(recovery = stats) } }
            }
        }
        try {
            status("Configuring ${rate / 1000} kHz · waiting for recorder confirmation")
            send(connected, "mode=standby", capture)
            send(connected, "samplerate=$rate", capture)
            send(connected, "gain=6", capture)
            send(connected, "openlead=0", capture)
            send(connected, "", capture, read = true)
            withTimeout(8000) { confirmation.await() }
            send(connected, "mode=run", capture, onWritten = { capture.confirmed.set(true); liveness.started(SystemClock.elapsedRealtime()) })
            keepalive = launch {
                try {
                    runSecaKeepalive {
                        val command = secaKeepaliveCommand(BuildConfig.SECA_EXPERIMENTAL_LED_KEEPALIVE, capture.ledStatus, SystemClock::elapsedRealtime)
                        send(connected, command.payload, capture, read = command.read, beforeWrite = { command.validateBeforeWrite(SystemClock.elapsedRealtime()) })
                    }
                }
                catch (cancelled: CancellationException) { throw cancelled }
                catch (error: Exception) { watchdogCause.compareAndSet(null, error.message); closeSocket() }
            }
            status("Recording ${rate / 1000} kHz · 12 leads · saved on phone" +
                (if (BuildConfig.SECA_EXPERIMENTAL_LED_KEEPALIVE) " · LED traffic experiment · contact detection off" else "") +
                (if (BuildConfig.SECA_EXPERIMENTAL_RECOVERY) " · framing recovery enabled" else ""))
            reader.await()?.let { throw it } // Watchdogs and explicit stop close the socket.
        } catch (error: Exception) {
            endReason = if (!graph.secaSettings.desired) "user_stop" else watchdogCause.get() ?: error.message?.take(180) ?: error.javaClass.simpleName
            watchdogCause.get()?.let { throw IOException(it, error) }
            throw error
        } finally {
            location.cancel()
            keepalive?.cancel()
            watchdog.cancel()
            closeSocket()
            reader.cancel()
            withContext(NonCancellable) {
                // join is bounded: socket closure unblocks read, withTimeout makes any
                // unexpected platform failure visible instead of hanging service cleanup.
                try { withTimeout(5000) { reader.join(); keepalive?.join(); consumer.join() } }
                catch (error: Exception) {
                    // The reader still owns its framer/journal. Freeze appends and keep
                    // the journal intact; do not race it by extracting or sealing here.
                    capture.freeze(error)
                    graph.secaSettings.stop()
                    val failure = ArchiveFailure(IOException("Bluetooth I/O cleanup timed out; journal retained; ${capture.ingress.diagnostic(SystemClock.elapsedRealtimeNanos())}", error))
                    report(failure)
                    status("Recording stopped; Bluetooth cleanup stalled")
                    throw failure
                }
                archiveFailure.get()?.let { throw ArchiveFailure(it) }
                if (!capture.ingress.drained()) {
                    val failure = IOException("seca archive did not drain; journal retained; ${capture.ingress.diagnostic(SystemClock.elapsedRealtimeNanos())}")
                    capture.freeze(failure)
                    throw ArchiveFailure(failure)
                }
                // GPS is auxiliary: a slow platform listener cleanup must not
                // mislabel a successfully drained ECG journal as an archive failure.
                try { withTimeout(4000) { location.join() } }
                catch (error: Exception) {
                    Log.e(TAG, "GPS cleanup timed out", error)
                    SecaStatus.update { it.copy(location = "GPS cleanup timed out; see diagnostics") }
                }
                capture.finish(endReason)
                activeCapture = null
            }
        }
    }

    private suspend fun send(connected: BluetoothSocket, text: String, capture: Capture?, read: Boolean = false, onWritten: () -> Unit = {}, beforeWrite: () -> Unit = {}) = coroutineScope {
        val bytes = SecaProtocol.command(text, read)
        val deadline = launch { delay(3000); try { connected.close() } catch (error: IOException) { Log.w(TAG, "seca write timeout cleanup", error) } }
        try {
            withTimeout(3000) {
                writeLock.withLock {
                    capture?.ingress?.transmit(bytes)?.await()
                    beforeWrite()
                    connected.outputStream.write(bytes)
                    connected.outputStream.flush()
                    onWritten()
                }
            }
        } finally { deadline.cancel() }
        delay(200) // ct331 requires distinct command writes with spacing.
    }

    private inner class Capture(device: String, rate: Int) {
        val confirmed = AtomicBoolean(false)
        val ledStatus = SecaLedStatus()
        val ingress = SecaIngress({ System.currentTimeMillis() * 1_000_000 to SystemClock.elapsedRealtimeNanos() },
            recoveryRate = if (BuildConfig.SECA_EXPERIMENTAL_RECOVERY) rate else null)
        private var header = SecaBatch(installation_id = graph.identity.installationId, session_id = UUID.randomUUID().toString(), batch_index = 0, device_id = device, sample_rate_hz = rate)
        val sessionId get() = header.session_id
        @Volatile private var broken: Exception? = null
        private var journal = archive { graph.secaQueue.begin(header) }
        private var since = SystemClock.elapsedRealtime()
        init { SecaStatus.update { it.copy(session = header.session_id, recovery = ingress.recoveryStats()) } }
        @Synchronized fun append(record: SecaIngress.Record) = archive {
            broken?.let { throw it }
            journal.append(record.bytes, record.direction, record.unixNs, record.monotonicNs, record.acquisition)
            if (journal.shouldSeal() || SystemClock.elapsedRealtime() - since >= 2000) {
                journal.finish()
                SecaStatus.update { it.copy(queued = graph.secaQueue.pending().size) }
                header = header.copy(batch_index = header.batch_index + 1)
                journal = graph.secaQueue.begin(header)
                since = SystemClock.elapsedRealtime()
            }
        }
        @Synchronized fun finish(reason: String) = archive {
            broken?.let { throw it } // Leave the entire journal for recovery after any disk error.
            if (!journal.closed) journal.finish(reason)
            SecaStatus.update { it.copy(queued = graph.secaQueue.pending().size) }
        }
        fun freeze(error: Exception) { broken = error }
        private fun <T> archive(block: () -> T): T = try { block() } catch (error: Exception) { broken = error; throw ArchiveFailure(error) }
    }

    private class ArchiveFailure(cause: Exception) : IOException("seca archive failed; recording stopped: ${cause.message}", cause)

    private fun locationPermitted() = RequiredPermissions.missingLocation(this).isEmpty()

    private fun refreshForegroundTypes(allowLocation: Boolean = true) {
        if (destroyed) return
        val types = if (Build.VERSION.SDK_INT < 29) 0 else
            ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE or
                (if (allowLocation && graph.settings.locationCollectionEnabled && locationPermitted()) ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION else 0)
        if (types == appliedForegroundTypes) return
        ServiceCompat.startForeground(this, NOTIFICATION, notification(SecaStatus.updates.value.state), types)
        appliedForegroundTypes = types
    }

    private suspend fun superviseLocation(sessionId: String) {
        // Same session UUID as seca, but the source is the phone, never a BLE ECG device.
        val writer = RawRecordWriter(graph.queue, CaptureContext(sessionId, UUID.randomUUID().toString()))
        var lastGate: String? = null
        while (currentCoroutineContext().isActive && graph.secaSettings.desired) {
            try {
                withContext(Dispatchers.Main) { refreshForegroundTypes() }
                val gate = when {
                    !graph.settings.locationCollectionEnabled -> "location_collection_disabled"
                    !locationPermitted() -> "location_permission_missing"
                    else -> null
                }
                if (gate != null) {
                    val detail = if (gate == "location_collection_disabled") "GPS off; enable location on the main screen" else "GPS needs location permission on the main screen"
                    SecaStatus.update { it.copy(location = detail) }
                    if (gate != lastGate) writer.event(gate, "warn", detail)
                    lastGate = gate
                    delay(2000)
                    continue
                }
                lastGate = null
                SecaStatus.update { it.copy(location = "GPS enabled; waiting for location ownership/fix") }
                graph.locationOwnership.collect({
                    graph.secaSettings.desired && graph.settings.locationCollectionEnabled && locationPermitted()
                }) {
                    LocationCollector(this, graph.settings, writer,
                        active = { graph.secaSettings.desired },
                        onFixStored = { provider -> SecaStatus.update { it.copy(location = "GPS: $provider fix saved at ${java.time.Instant.now()}") } },
                    ).run()
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                Log.e(TAG, "seca GPS collection failed", error)
                SecaStatus.update { it.copy(location = "GPS failed; retrying: ${error.message}") }
                try { writer.event("location_collection_failed", "error", error.message ?: error.javaClass.simpleName) }
                catch (cancelled: CancellationException) { throw cancelled }
                catch (eventError: Exception) { Log.e(TAG, "Could not persist GPS failure", eventError) }
                delay(10_000)
            }
        }
    }
    private fun closeSocket() { try { socket?.close() } catch (error: IOException) { Log.w(TAG, "seca socket cleanup failed", error) } }
    private fun report(error: Exception) { Log.e(TAG, "seca recording error", error); SecaStatus.update { it.copy(error = error.message ?: error.javaClass.simpleName) } }
    private fun status(text: String) { SecaStatus.update { it.copy(state = text) }; getSystemService(NotificationManager::class.java).notify(NOTIFICATION, notification(text)) }
    private fun notification(text: String) = NotificationCompat.Builder(this, CHANNEL)
        .setSmallIcon(R.drawable.ic_sensor).setContentTitle("seca ECG").setContentText(text).setOngoing(true)
        .setContentIntent(PendingIntent.getActivity(this, 2, Intent(this, SecaActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT))
        .addAction(0, "Stop", PendingIntent.getService(this, 3, Intent(this, SecaService::class.java).setAction(STOP), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)).build()

    override fun onDestroy() { destroyed = true; closeSocket(); scope.cancel(); super.onDestroy() }
    companion object {
        private const val TAG = "SecaService"
        private const val CHANNEL = "seca_recording"
        private const val NOTIFICATION = 331
        private const val STOP = "seca.stop"
        private val SPP = UUID.fromString("00001101-0000-1000-8000-00805f9b34fb")
        fun start(context: Context) { ContextCompat.startForegroundService(context, Intent(context, SecaService::class.java)) }
        fun stop(context: Context) {
            (context.applicationContext as SensorsApplication).graph.secaSettings.stop()
            if (SecaStatus.updates.value.running) context.startService(Intent(context, SecaService::class.java).setAction(STOP))
        }
    }
}
