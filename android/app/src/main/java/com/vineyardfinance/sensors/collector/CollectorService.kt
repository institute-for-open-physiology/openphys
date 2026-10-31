package com.vineyardfinance.sensors.collector

import android.Manifest
import android.app.Notification
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.net.ConnectivityManager
import android.net.Network
import android.os.IBinder
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import com.vineyardfinance.sensors.MainActivity
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.R
import com.vineyardfinance.sensors.SensorsApplication
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.data.RecordJson
import com.vineyardfinance.sensors.location.LocationCollector
import com.vineyardfinance.sensors.network.UploadLoopSupervisor
import com.vineyardfinance.sensors.network.UploadWorker
import com.vineyardfinance.sensors.setup.RequiredPermissions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.UUID

class CollectorService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val runtimeSequencer = CollectorRuntimeSequencer<Job>(
        isActive = { it.isActive },
        isCompleted = { it.isCompleted },
        cancel = { it.cancel() },
    )
    private val serviceStartIdGuard = ServiceStartIdGuard { startId -> stopSelfResult(startId) }
    private val wakeLockStatusOverlay = WakeLockStatusOverlay()
    private val writerOwner = LifecycleTokenOwner<RawRecordWriter>()
    private val wakeLockOwner = LifecycleTokenOwner<WakeLockLease>()
    private val networkCallbackOwner = LifecycleTokenOwner<ConnectivityManager.NetworkCallback>()
    private val graph get() = (application as SensorsApplication).graph
    private var notificationOwner: NotificationOwner? = null
    private var statusNotificationBridge: StatusNotificationBridge? = null
    private var notificationSink: LifecycleNotificationSink? = null
    private lateinit var lifecycleHandler: Handler
    private var preservePreRunActionNeeded = false
    private var userStopRequested = false
    @Volatile private var fatalFailureLatched = false
    @Volatile private var lifecycleResourcesOpen = true
    @Volatile private var appliedForegroundTypes: Int? = null

    override fun onCreate() {
        super.onCreate()
        CollectorNotificationChannel.ensure(this)
        val persistedFailure = graph.settings.collectorFailureDetail
        fatalFailureLatched = persistedFailure != null
        CollectorStatus.update { current ->
            if (persistedFailure != null) {
                CollectorRuntimePolicy.failedStatus(current, persistedFailure)
            } else {
                current.transitionTo("Starting", "Checking collector setup")
                    .copy(serviceRunning = true)
            }
        }
        lifecycleHandler = Handler(Looper.getMainLooper())
        notificationSink = LifecycleNotificationSink(
            isDispatchThread = { Looper.myLooper() == Looper.getMainLooper() },
            dispatch = handlerFunctionDispatcher(lifecycleHandler),
            onNotify = { content ->
                runCatching {
                    getSystemService(NotificationManager::class.java).notify(NOTIFICATION_ID, notification(content))
                }.onFailure { error ->
                    Log.e(TAG, "Unable to update collector notification", error)
                }
            },
        )
        notificationOwner = NotificationOwner(
            onNotify = notificationSink?.let { sink -> sink::offer }
                ?: error("notification sink missing"),
            initialSnapshot = CollectorNotificationMapper.toOwnerSnapshot(
                CollectorStatus.updates.value,
                System.currentTimeMillis(),
            ),
        )
        startTruthfulForeground()
        notificationOwner?.markCurrentContentPosted()
        val mapper = CollectorNotificationMapper(notificationOwner ?: error("notification owner missing"))
        statusNotificationBridge = StatusNotificationBridge(
            subscribe = CollectorStatus::observeNotifications,
            mapper = mapper,
        ).also { it.onCreate() }
        notificationSink?.drainOnLifecycleThread()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        serviceStartIdGuard.commandDelivered(startId)
        if (intent?.action == ACTION_STOP) {
            userStopRequested = true
            fatalFailureLatched = false
            runCatching { graph.settings.recordCollectorStopped() }.onFailure { error ->
                CollectorStatus.update {
                    it.copy(lastError = "Failed to persist collector stop: ${safe(error)}")
                }
            }
            preservePreRunActionNeeded = false
            if (runtimeSequencer.requestStop() == CollectorRuntimeSequencer.StopDecision.STOP_NOW) {
                serviceStartIdGuard.stopIfLatest()
            }
            return START_NOT_STICKY
        }
        val persistedFailure = graph.settings.collectorFailureDetail
        val startDecision = CollectorRuntimePolicy.onStartCommand(
            fatalFailureLatched || persistedFailure != null,
        )
        if (!startDecision.startRuntime) {
            val detail = persistedFailure ?: CollectorStatus.updates.value.lastError
                ?: "Collector stopped after an unexpected failure"
            fatalFailureLatched = true
            CollectorStatus.update { CollectorRuntimePolicy.failedStatus(it, detail) }
            notificationSink?.drainOnLifecycleThread()
            return START_NOT_STICKY
        }
        if (runtimeSequencer.hasActiveRuntime()) return START_STICKY
        val setup = graph.setup.inspect(
            CollectorStatus.updates.value,
            allowDebugCredential = BuildConfig.DEBUG,
        )
        val credentialAvailable = BuildConfig.DEBUG || graph.credentials.currentCredential() != null
        if (!CollectorAdmission.prerequisitesSatisfied(credentialAvailable, hasBlePermissions(), setup)) {
            CollectorStatus.update {
                it.transitionTo(
                    state = "Action needed",
                    detail = setup.firstIncomplete?.detail ?: "Complete phone setup before collecting",
                ).copy(serviceRunning = true)
            }
            preservePreRunActionNeeded = true
            notificationSink?.drainOnLifecycleThread()
            if (runtimeSequencer.requestStop() == CollectorRuntimeSequencer.StopDecision.STOP_NOW) {
                serviceStartIdGuard.stopIfLatest()
            }
            return START_NOT_STICKY
        }
        graph.settings.collectionEnabled = true
        userStopRequested = false
        preservePreRunActionNeeded = false
        return if (requestRuntimeStart() == CollectorRuntimeSequencer.StartDecision.CLOSED) {
            START_NOT_STICKY
        } else {
            START_STICKY
        }
    }

    private fun requestRuntimeStart(): CollectorRuntimeSequencer.StartDecision =
        runtimeSequencer.requestStart {
            val job = scope.launch { runCollector() }
            job.invokeOnCompletion {
                lifecycleHandler.post { onRuntimeCompleted(job) }
            }
            job
        }

    private fun onRuntimeCompleted(job: Job) {
        when (runtimeSequencer.completed(job)) {
            CollectorRuntimeSequencer.CompletionDecision.IGNORE -> Unit
            CollectorRuntimeSequencer.CompletionDecision.STOP_SERVICE -> {
                val decision = if (fatalFailureLatched) {
                    CollectorRuntimePolicy.afterFatalFailure(graph.settings.collectionEnabled)
                } else {
                    CollectorRuntimePolicy.afterCleanStop()
                }
                if (decision.keepForeground) {
                    notificationSink?.drainOnLifecycleThread()
                } else {
                    serviceStartIdGuard.stopIfLatest()
                }
            }
            CollectorRuntimeSequencer.CompletionDecision.RESTART -> {
                if (fatalFailureLatched || graph.settings.collectorFailureDetail != null) {
                    notificationSink?.drainOnLifecycleThread()
                } else {
                    requestRuntimeStart()
                }
            }
        }
    }

    private suspend fun runCollector() {
        var localWriter: RawRecordWriter? = null
        var localWakeLock: WakeLockLease? = null
        var localNetworkCallback: ConnectivityManager.NetworkCallback? = null
        var uploadJob: Job? = null
        var locationJob: Job? = null
        var failureDetail: String? = null
        try {
            val configuredTarget = H10Identity.configuredSerial(graph.settings.target)
            val captureContext = CaptureContext(
                captureSessionId = UUID.randomUUID().toString(),
                writerEpochId = UUID.randomUUID().toString(),
                physicalDeviceId = H10Identity.physicalDeviceId(configuredTarget),
            )
            val runtimeWriter = RawRecordWriter(graph.queue, captureContext, graph.signals)
            localWriter = runtimeWriter
            if (!writerOwner.install(runtimeWriter)) {
                throw CancellationException("collector service closed before writer publication")
            }
            ensureSetupCurrent(writerOwner, runtimeWriter, "writer publication")
            localWakeLock = acquireWakeLock()
            ensureSetupCurrent(wakeLockOwner, localWakeLock, "wake-lock acquisition")
            startWakeLockRenewal(runtimeWriter, localWakeLock)
            localNetworkCallback = registerNetworkTelemetry(runtimeWriter)
            ensureSetupCurrent(networkCallbackOwner, localNetworkCallback, "network callback registration")
            runtimeWriter.event(
                "android_service_started",
                "info",
                "foreground connectedDevice service",
                RecordJson.fields(
                    mapOf(
                        "configured_h10_serial" to configuredTarget,
                        "physical_device_id" to captureContext.physicalDeviceId,
                    ),
                ),
            )
            uploadJob = if (graph.credentials.currentCredential() != null) scope.launch {
                var lastAttemptEventMs = 0L
                var lastAckEventMs = 0L
                val onStatus: suspend (String) -> Unit = { message ->
                    CollectorStatus.update {
                        when {
                            "ACK" in message -> it.copy(
                                lastAck = message,
                                lastAckUnixMs = System.currentTimeMillis(),
                                lastError = null,
                            )
                            "paused" in message.lowercase() -> it.copy(lastError = message.take(240))
                            else -> it
                        }
                    }
                    val now = System.currentTimeMillis()
                    val isError = "paused" in message.lowercase()
                    val isAck = "ACK" in message
                    val shouldRecord = isError || (isAck && now - lastAckEventMs >= UPLOAD_TELEMETRY_INTERVAL_MS) ||
                        (!isAck && now - lastAttemptEventMs >= UPLOAD_TELEMETRY_INTERVAL_MS)
                    if (shouldRecord) {
                        runtimeWriter.event(
                            event = if (isAck) "upload_durable_ack" else if (isError) "upload_error" else "upload_attempt",
                            severity = if (isError) "error" else "info",
                            detail = message,
                        )
                        if (isAck) lastAckEventMs = now else lastAttemptEventMs = now
                    }
                }
                UploadLoopSupervisor(
                    vitals = graph.uploader.vitals,
                    queuedRecords = { graph.queue.backlog().records },
                    clock = android.os.SystemClock::elapsedRealtime,
                    onStall = { report ->
                        runtimeWriter.event(
                            "upload_stall_detected",
                            "critical",
                            "no upload progress with records queued; restarting upload loop",
                            RecordJson.fields(
                                mapOf(
                                    "queued_records" to report.queuedRecords,
                                    "progress_age_ms" to report.progressAgeMs,
                                    "attempt_age_ms" to report.attemptAgeMs,
                                    "ack_age_ms" to report.ackAgeMs,
                                    "backoff_ms" to report.backoffMs,
                                    "consecutive_failures" to report.consecutiveFailures,
                                    "last_outcome" to report.lastOutcome,
                                ),
                            ),
                        )
                    },
                    onLoopFailure = { detail ->
                        runtimeWriter.event("upload_loop_failed", "critical", detail)
                    },
                ).supervise(active = { graph.settings.collectionEnabled }) {
                    graph.uploader.retryLoop(
                        active = { graph.settings.collectionEnabled },
                        onStatus = onStatus,
                    )
                }
            } else scope.launch {
                runtimeWriter.event(
                    "upload_disabled_local_debug",
                    "warn",
                    "debug-only local collection; no data leaves phone",
                )
            }
            locationJob = scope.launch {
                // A dedicated writer keeps location records free of the H10's
                // BLE connection/peripheral/device provenance: GPS comes from
                // the phone, not the chest strap.
                superviseLocationCollection(
                    RawRecordWriter(graph.queue, captureContext.copy(physicalDeviceId = null)),
                )
            }
            H10Collector(this, graph.settings, runtimeWriter).run()
            check(!graph.settings.collectionEnabled) {
                "collector runtime ended unexpectedly while collection remained enabled"
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            val detail = safe(error)
            failureDetail = detail
            localWriter?.let { writer ->
                runCatching { writer.event("android_service_fatal", "critical", detail) }
            }
            if (graph.settings.collectionEnabled) latchFatalFailure(detail)
        } finally {
            uploadJob?.cancel()
            locationJob?.cancel()
            var cleanupFailure: String? = null
            withContext(NonCancellable) {
                val cleanupError = localWriter?.let { writerForCleanup ->
                    runCatching {
                        withTimeout(SERVICE_CLEANUP_TIMEOUT_MS) {
                            writerForCleanup.event(
                                "android_service_stopped",
                                "info",
                                "collector runtime ended",
                            )
                        }
                    }.exceptionOrNull()
                }
                if (cleanupError != null) {
                    cleanupFailure = "Service cleanup record failed: ${safe(cleanupError)}"
                }
                runCatching { unregisterNetworkTelemetry(localNetworkCallback) }.onFailure { error ->
                    cleanupFailure = cleanupFailure ?: "Network cleanup failed: ${safe(error)}"
                }
                runCatching { releaseWakeLock(localWakeLock) }.onFailure { error ->
                    cleanupFailure = cleanupFailure ?: "Wake-lock cleanup failed: ${safe(error)}"
                }
            }
            localWriter?.let(writerOwner::release)
            val cleanupResolution = CollectorRuntimePolicy.resolveCleanup(
                primaryFailure = failureDetail,
                cleanupFailure = cleanupFailure,
                collectionDesired = graph.settings.collectionEnabled,
            )
            failureDetail = cleanupResolution.failureDetail
            cleanupResolution.fatalCleanupFailure?.let(::latchFatalFailure)
            val decision = failureDetail?.let {
                CollectorRuntimePolicy.afterFatalFailure(graph.settings.collectionEnabled)
            } ?: CollectorRuntimePolicy.afterCleanStop()
            if (decision.scheduleResidualUpload) {
                runCatching { UploadWorker.enqueue(this@CollectorService) }.onFailure { error ->
                    CollectorStatus.update {
                        it.copy(lastError = "Residual upload scheduling failed: ${safe(error)}")
                    }
                }
            }
            withContext(NonCancellable) {
                drainNotificationDispatch()
            }
        }
    }

    /**
     * Runs consent-gated location collection alongside the H10 runtime.
     *
     * The per-installation location opt-in and the runtime permission are the
     * gates: while either is missing, no [LocationCollector] exists and only a
     * single explanatory (location-free) event is recorded so the gap stays
     * explainable. The poll delay bounds every await in the disabled branch;
     * LocationCollector bounds its own waits internally.
     */
    private suspend fun superviseLocationCollection(locationWriter: RawRecordWriter) {
        var announcedDisabled = false
        var announcedPermissionMissing = false
        while (currentCoroutineContext().isActive && graph.settings.collectionEnabled) {
            val consent = graph.settings.locationCollectionEnabled
            val permitted = hasLocationPermissions()
            if (consent && permitted) {
                announcedDisabled = false
                announcedPermissionMissing = false
                refreshForegroundServiceTypes()
                try {
                    graph.locationOwnership.collect({
                        graph.settings.collectionEnabled && graph.settings.locationCollectionEnabled && hasLocationPermissions()
                    }) { LocationCollector(this, graph.settings, locationWriter).run() }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Throwable) {
                    // Location is auxiliary: never take down H10 collection,
                    // but fail loudly and retry after a backoff.
                    runCatching {
                        locationWriter.event("location_collection_failed", "error", safe(error))
                    }.onFailure { eventError ->
                        Log.e(TAG, "Unable to record location collection failure", eventError)
                    }
                    delay(LOCATION_RETRY_MS)
                } finally {
                    refreshForegroundServiceTypes()
                }
            } else {
                if (consent && !permitted && !announcedPermissionMissing) {
                    announcedPermissionMissing = true
                    // This branch is the default path for every non-consenting install; a
                    // failed telemetry append must not crash the collector process.
                    runCatching { locationWriter.event(
                        "location_permission_missing",
                        "warn",
                        "location opt-in is on but the Android location permission is not granted; no location records",
                    ) }
                }
                if (!consent && !announcedDisabled) {
                    announcedDisabled = true
                    runCatching { locationWriter.event(
                        "location_collection_disabled",
                        "info",
                        "per-installation location opt-in is off; no location records this session",
                    ) }
                }
                delay(LOCATION_GATE_POLL_MS)
            }
        }
    }

    private fun hasLocationPermissions(): Boolean = RequiredPermissions.missingLocation(this).isEmpty()

    private fun foregroundServiceTypes(): Int {
        if (Build.VERSION.SDK_INT < 29) return 0
        var types = ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE
        // The location type may only be claimed when its prerequisites hold
        // (targetSdk 34 throws otherwise), and claiming it without the user's
        // opt-in would be dishonest about what the service does.
        if (graph.settings.locationCollectionEnabled && hasLocationPermissions()) {
            types = types or ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION
        }
        return types
    }

    private fun refreshForegroundServiceTypes() {
        lifecycleHandler.post {
            if (!lifecycleResourcesOpen) return@post
            if (foregroundServiceTypes() == appliedForegroundTypes) return@post
            runCatching { startTruthfulForeground() }.onFailure { error ->
                Log.e(TAG, "Unable to update foreground service types", error)
                CollectorStatus.update {
                    it.copy(lastError = "Foreground service type update failed: ${safe(error)}")
                }
            }
        }
    }

    private fun latchFatalFailure(detail: String) {
        fatalFailureLatched = true
        runCatching { graph.settings.collectorFailureDetail = detail }.onFailure { error ->
            CollectorStatus.update {
                it.copy(lastError = "Failure-state persistence failed: ${safe(error)}")
            }
        }
        CollectorStatus.update { CollectorRuntimePolicy.failedStatus(it, detail) }
    }

    private fun acquireWakeLock(): WakeLockLease {
        val power = getSystemService(PowerManager::class.java)
        val platformWakeLock = power.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "$packageName:collector").apply {
            setReferenceCounted(false)
        }
        val lease = WakeLockLease(object : TimedWakeLock {
            override val isHeld: Boolean get() = platformWakeLock.isHeld
            override fun acquire(timeoutMs: Long) = platformWakeLock.acquire(timeoutMs)
            override fun release() = platformWakeLock.release()
        }, WAKE_LOCK_TIMEOUT_MS)
        if (!wakeLockOwner.isOpen()) {
            throw CancellationException("collector service closed before wake-lock acquisition")
        }
        lease.start()
        if (!wakeLockOwner.installOrCleanup(
                lease,
                onInstalled = wakeLockStatusOverlay::activate,
                cleanupRejected = WakeLockLease::stop,
            )
        ) {
            throw CancellationException("collector service closed during wake-lock acquisition")
        }
        // Return immediately after publication. The caller must assign its exact
        // cleanup token before any cancellable or otherwise throwing setup work.
        return lease
    }

    private fun startWakeLockRenewal(localWriter: RawRecordWriter, lease: WakeLockLease) {
        scope.launch {
            if (!wakeLockEventAllowed(lease)) return@launch
            runCatching { localWriter.event("wake_lock_acquired", "info", "partial wake lock", RecordJson.fields(mapOf("timeout_ms" to WAKE_LOCK_TIMEOUT_MS))) }
            while (graph.settings.collectionEnabled && wakeLockOwner.isCurrent(lease)) {
                delay(WAKE_LOCK_RENEW_MS)
                while (graph.settings.collectionEnabled && wakeLockOwner.isCurrent(lease)) {
                    val renewalError = runCatching {
                        check(lease.renew()) { "wake-lock lease stopped before renewal" }
                    }.exceptionOrNull()
                    if (!wakeLockEventAllowed(lease)) break
                    if (renewalError == null) {
                        CollectorStatus.update { wakeLockStatusOverlay.renewalSucceeded(lease, it) }
                        runCatching {
                            localWriter.event(
                                "wake_lock_renewed",
                                "debug",
                                "partial wake lock",
                                RecordJson.fields(mapOf("timeout_ms" to WAKE_LOCK_TIMEOUT_MS)),
                            )
                        }
                        break
                    }
                    val detail = (renewalError.message ?: renewalError.javaClass.simpleName).take(240)
                    runCatching { localWriter.event("wake_lock_renewal_failed", "critical", detail) }
                    CollectorStatus.update { wakeLockStatusOverlay.renewalFailed(lease, it, detail) }
                    delay(WAKE_LOCK_RETRY_MS)
                }
            }
        }
    }

    private fun wakeLockEventAllowed(lease: WakeLockLease): Boolean =
        lifecycleTokenActiveAndCurrent(
            wakeLockOwner,
            lease,
            runtimeActive = lifecycleResourcesOpen && scope.coroutineContext.isActive,
        )

    private fun releaseWakeLock(lease: WakeLockLease?) {
        val ownedLease = lease?.let(wakeLockOwner::release) ?: return
        cleanupWakeLock(ownedLease)
    }

    private fun cleanupWakeLock(lease: WakeLockLease) {
        CollectorStatus.update { wakeLockStatusOverlay.release(lease, it) }
        lease.stop()
    }

    private fun registerNetworkTelemetry(localWriter: RawRecordWriter): ConnectivityManager.NetworkCallback {
        if (!networkCallbackOwner.isOpen()) {
            throw CancellationException("collector service closed before network callback registration")
        }
        val connectivity = getSystemService(ConnectivityManager::class.java)
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                publishNetworkEvent(this, localWriter, "network_available", "info", "network became available")
            }

            override fun onLost(network: Network) {
                publishNetworkEvent(this, localWriter, "network_lost", "warn", "network lost; records remain queued")
            }
        }
        connectivity.registerDefaultNetworkCallback(callback)
        if (!networkCallbackOwner.installOrCleanup(
                callback,
                cleanupRejected = {
                    runCatching { unregisterPlatformNetworkCallback(connectivity, it) }
                        .onFailure { error ->
                            Log.e(TAG, "Unable to unregister rejected network telemetry", error)
                        }
                },
            )
        ) {
            throw CancellationException("collector service closed during network callback registration")
        }
        // Hand the exact token to runCollector before its next cancellation
        // gate so normal STOP cleanup cannot lose the registered callback.
        return callback
    }

    private fun publishNetworkEvent(
        callback: ConnectivityManager.NetworkCallback,
        localWriter: RawRecordWriter,
        event: String,
        severity: String,
        detail: String,
    ) {
        if (!networkEventAllowed(callback)) return
        scope.launch {
            if (networkEventAllowed(callback)) {
                localWriter.event(event, severity, detail)
            }
        }
    }

    private fun networkEventAllowed(callback: ConnectivityManager.NetworkCallback): Boolean =
        lifecycleTokenActiveAndCurrent(
            networkCallbackOwner,
            callback,
            runtimeActive = lifecycleResourcesOpen && scope.coroutineContext.isActive,
        )

    private fun unregisterNetworkTelemetry(callback: ConnectivityManager.NetworkCallback?) {
        callback ?: return
        networkCallbackOwner.releaseAfterCleanup(callback) { registered ->
            unregisterPlatformNetworkCallback(
                getSystemService(ConnectivityManager::class.java),
                registered,
            )
        }
    }

    private fun unregisterPlatformNetworkCallback(
        connectivity: ConnectivityManager,
        callback: ConnectivityManager.NetworkCallback,
    ) {
        connectivity.unregisterNetworkCallback(callback)
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        writerOwner.current()?.let { currentWriter ->
            scope.launch {
                if (lifecycleTokenActiveAndCurrent(writerOwner, currentWriter, lifecycleResourcesOpen)) {
                    runCatching { currentWriter.event("android_task_removed", "info", "UI task removed; service continues") }
                }
            }
        }
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        lifecycleResourcesOpen = false
        runtimeSequencer.close()
        writerOwner.close()
        val wakeLock = wakeLockOwner.close()
        var destroyError: String? = null
        runCatching {
            networkCallbackOwner.closeAfterCleanup {
                unregisterPlatformNetworkCallback(getSystemService(ConnectivityManager::class.java), it)
            }
        }.onFailure { error ->
            Log.e(TAG, "Unable to unregister network telemetry during destroy", error)
            destroyError = "Network cleanup failed: ${safe(error)}"
        }
        wakeLock?.let {
            runCatching { cleanupWakeLock(it) }.onFailure { error ->
                destroyError = destroyError ?: "Wake-lock cleanup failed: ${safe(error)}"
            }
        }
        CollectorStatus.update { status ->
            val preserveAction = !userStopRequested &&
                ((preservePreRunActionNeeded && status.state == "Action needed") ||
                    status.state == "Collection failed" || graph.settings.collectionEnabled)
            if (preserveAction) {
                status.copy(
                    serviceRunning = false,
                    connected = false,
                    lastError = destroyError ?: status.lastError,
                )
            } else {
                status.transitionTo("Stopped", "Collection disabled")
                    .copy(serviceRunning = false, lastError = destroyError)
            }
        }
        notificationSink?.drainOnLifecycleThread()
        statusNotificationBridge?.close()
        statusNotificationBridge = null
        notificationSink?.close()
        notificationSink = null
        runCatching { stopForeground(STOP_FOREGROUND_REMOVE) }.onFailure { error ->
            destroyError = destroyError ?: "Foreground cleanup failed: ${safe(error)}"
        }
        runCatching {
            getSystemService(NotificationManager::class.java).cancel(NOTIFICATION_ID)
        }.onFailure { error ->
            destroyError = destroyError ?: "Notification cleanup failed: ${safe(error)}"
        }
        destroyError?.let { detail ->
            CollectorStatus.update { it.copy(lastError = detail) }
        }
        scope.cancel()
        super.onDestroy()
    }

    private suspend fun <T : Any> ensureSetupCurrent(
        owner: LifecycleTokenOwner<T>,
        token: T,
        stage: String,
    ) {
        val context = currentCoroutineContext()
        context.ensureActive()
        if (!lifecycleTokenActiveAndCurrent(owner, token, context.isActive && lifecycleResourcesOpen)) {
            throw CancellationException("collector service closed during $stage")
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun startTruthfulForeground() {
        val owner = notificationOwner ?: error("notification owner missing")
        val types = foregroundServiceTypes()
        ServiceCompat.startForeground(
            this,
            NOTIFICATION_ID,
            notification(owner.content()),
            types,
        )
        appliedForegroundTypes = types
    }

    private fun notification(content: NotificationContent): Notification {
        val open = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val stop = PendingIntent.getService(this, 1, Intent(this, CollectorService::class.java).setAction(ACTION_STOP), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val builder = NotificationCompat.Builder(this, CollectorNotificationChannel.ID)
            .setSmallIcon(R.drawable.ic_sensor)
            .setContentTitle(content.title)
            .setContentText(content.text)
            .setStyle(NotificationCompat.BigTextStyle().bigText(content.text))
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setContentIntent(open)
        if (content.showPairingAction) builder.addAction(0, "Confirm pairing", open)
        return builder.addAction(0, "Stop", stop).build()
    }

    /** Drains queued notification decisions outside the serialized status monitor. */
    private suspend fun drainNotificationDispatch() = withContext(Dispatchers.Main.immediate) {
        notificationSink?.drainOnLifecycleThread()
    }

    private fun hasBlePermissions(): Boolean = Build.VERSION.SDK_INT < 31 ||
        (ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED)

    private fun safe(error: Throwable): String =
        (error.message ?: error.javaClass.simpleName).take(240)

    companion object {
        const val ACTION_START = "com.vineyardfinance.sensors.action.START"
        const val ACTION_STOP = "com.vineyardfinance.sensors.action.STOP"
        private const val NOTIFICATION_ID = 1001
        private const val WAKE_LOCK_TIMEOUT_MS = 10 * 60 * 1_000L
        private const val WAKE_LOCK_RENEW_MS = 9 * 60 * 1_000L
        private const val WAKE_LOCK_RETRY_MS = 5_000L
        private const val UPLOAD_TELEMETRY_INTERVAL_MS = 60_000L
        private const val SERVICE_CLEANUP_TIMEOUT_MS = 3_000L
        private const val LOCATION_GATE_POLL_MS = 2_000L
        private const val LOCATION_RETRY_MS = 30_000L
        private const val TAG = "CollectorService"

        fun start(context: Context) = ContextCompat.startForegroundService(context, Intent(context, CollectorService::class.java).setAction(ACTION_START))
        fun stop(context: Context) = context.startService(Intent(context, CollectorService::class.java).setAction(ACTION_STOP))
    }
}
