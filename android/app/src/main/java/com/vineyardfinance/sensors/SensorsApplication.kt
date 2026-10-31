package com.vineyardfinance.sensors

import android.app.Application
import android.os.Build
import android.util.Log
import com.vineyardfinance.sensors.auth.EncryptedDeviceCredentialStore
import com.vineyardfinance.sensors.auth.EnrollmentClient
import com.vineyardfinance.sensors.auth.EnrollmentManager
import com.vineyardfinance.sensors.auth.EnrollmentMetadata
import com.vineyardfinance.sensors.auth.PostEnrollmentRecovery
import com.vineyardfinance.sensors.collector.BacklogStatusMonitor
import com.vineyardfinance.sensors.collector.CollectorService
import com.vineyardfinance.sensors.collector.CollectorStatus
import com.vineyardfinance.sensors.collector.withBacklog
import com.vineyardfinance.sensors.collector.withBacklogUnavailable
import com.vineyardfinance.sensors.data.AppDatabase
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.data.QueueRepository
import com.vineyardfinance.sensors.network.HttpJson
import com.vineyardfinance.sensors.network.IngestUploader
import com.vineyardfinance.sensors.network.UploadLoopVitals
import com.vineyardfinance.sensors.network.UploadWorker
import com.vineyardfinance.sensors.setup.AndroidSetupInspector
import com.vineyardfinance.sensors.update.AppUpdateClient
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import com.vineyardfinance.sensors.seca.SecaQueue
import com.vineyardfinance.sensors.seca.SecaSettings
import com.vineyardfinance.sensors.seca.SecaStatus
import com.vineyardfinance.sensors.seca.SecaUploader
import com.vineyardfinance.sensors.seca.SecaUploadWorker
import com.vineyardfinance.sensors.seca.SecaTiming
import com.vineyardfinance.sensors.seca.syncSecaDirectory
import java.io.File

class SensorsApplication : Application() {
    lateinit var graph: AppGraph
        private set

    override fun onCreate() {
        super.onCreate()
        graph = AppGraph(this)
        SecaUploadWorker.enqueue(this)
    }
}

class AppGraph(private val application: Application) {
    fun enqueueUpload() = UploadWorker.enqueue(application)

    private val applicationScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val settings = AppSettings(application)
    val locationOwnership = com.vineyardfinance.sensors.location.LocationOwnership()
    val identity = AppIdentity(application)
    val database = AppDatabase.build(application)
    val queue = QueueRepository(database, identity)
    val signalStore = com.vineyardfinance.sensors.data.SignalStore(application)
    val signals = com.vineyardfinance.sensors.collector.LiveSignals(signalStore)
    val http = HttpJson(BuildConfig.DEFAULT_SERVER_URL)
    val credentials = EncryptedDeviceCredentialStore(application)
    val secaSettings = SecaSettings(application)
    val secaQueue by lazy { SecaQueue(File(application.filesDir, "seca_queue"), operationTiming = SecaTiming::record, syncDirectory = ::syncSecaDirectory) }
    val secaUploader by lazy { SecaUploader(secaQueue, http, credentials) }
    // elapsedRealtime as the vitals clock base: it counts through Doze and never steps, so a
    // device clock change can neither fake nor mask an upload stall. The supervisor and the
    // collector_health ages read the same base.
    val uploader = IngestUploader(
        http,
        credentials,
        queue,
        vitals = UploadLoopVitals(android.os.SystemClock::elapsedRealtime),
    )
    val updates = AppUpdateClient(BuildConfig.DEFAULT_SERVER_URL)
    val setup = AndroidSetupInspector(application, settings, credentials, identity)
    private val backlogStatusMonitor = BacklogStatusMonitor(
        backlog = queue.observeBacklog(),
        publish = { backlog -> CollectorStatus.update { it.withBacklog(backlog) } },
        publishUnavailable = { detail ->
            Log.e(TAG, detail)
            runCatching {
                CollectorStatus.update { it.withBacklogUnavailable(detail) }
            }.onFailure { error ->
                Log.e(TAG, "Could not publish local queue monitor failure", error)
            }
        },
    )
    private val postEnrollmentRecovery = PostEnrollmentRecovery(
        collectionDesired = { settings.collectionEnabled },
        setupReady = { setup.inspect(CollectorStatus.updates.value).preStartComplete },
        enqueueResidualUpload = { UploadWorker.enqueue(application) },
        startCollector = { CollectorService.start(application) },
        reportFailure = { message -> CollectorStatus.update { it.copy(lastError = message) } },
    )
    val enrollmentClient = EnrollmentClient(http, identity, credentials) {
        EnrollmentMetadata(
            displayLabel = Build.MODEL.takeIf { it.isNotBlank() },
            androidManufacturer = Build.MANUFACTURER,
            androidModel = Build.MODEL,
            androidSdk = Build.VERSION.SDK_INT,
            appPackage = application.packageName,
            appVersionName = BuildConfig.VERSION_NAME,
            appVersionCode = BuildConfig.VERSION_CODE.toLong(),
        )
    }
    val enrollment = EnrollmentManager(
        enrollmentClient,
        applicationScope,
        onConnected = { postEnrollmentRecovery.resume() },
        passwordEnrollment = com.vineyardfinance.sensors.auth.PasswordEnrollmentClient(BuildConfig.DEFAULT_SERVER_URL),
    )

    val logout = com.vineyardfinance.sensors.auth.DeviceLogout(
        recording = { settings.collectionEnabled || CollectorStatus.updates.value.serviceRunning ||
            secaSettings.desired || SecaStatus.updates.value.running },
        pendingData = {
            kotlinx.coroutines.withContext(Dispatchers.IO) {
                queue.backlog().records > 0 || secaQueue.hasUnuploadedData()
            }
        },
        clearCredential = { credentials.clear() },
    )

    init {
        // Recover fsynced raw journals before a service or worker can append/upload.
        // A seca queue failure is visible without disturbing the independent H10.
        runCatching {
            val recovered = secaQueue.recover()
            SecaStatus.update { it.copy(queued = secaQueue.pending().size, error = if (recovered > 0) "$recovered interrupted seca chunk(s) recovered" else null) }
        }.onFailure { error ->
            Log.e(TAG, "seca journal recovery failed", error)
            runCatching { secaSettings.stop() }
            SecaStatus.update { it.copy(error = "seca recovery failed: ${error.message}") }
        }
        applicationScope.launch(CoroutineName("durable-backlog-status")) {
            backlogStatusMonitor.run()
        }
    }

    companion object {
        private const val TAG = "SensorsApp"
    }
}
