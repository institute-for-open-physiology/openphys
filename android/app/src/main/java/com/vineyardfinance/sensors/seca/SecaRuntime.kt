package com.vineyardfinance.sensors.seca

import android.content.Context
import android.system.Os
import android.system.OsConstants
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.vineyardfinance.sensors.SensorsApplication
import com.vineyardfinance.sensors.auth.DeviceCredentialProvider
import com.vineyardfinance.sensors.network.HttpJson
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

data class SecaSnapshot(
    val running: Boolean = false,
    val state: String = "Ready",
    val packets: Long = 0,
    val bytes: Long = 0,
    val queued: Int = 0,
    val uploaded: Long = 0,
    val session: String? = null,
    val error: String? = null,
    val uploadError: String? = null,
    val location: String = "GPS: waiting for recording",
    val locationUpload: String? = null,
    val recovery: SecaRecovery.Stats? = null,
)

object SecaStatus {
    private val mutable = MutableStateFlow(SecaSnapshot())
    val updates = mutable.asStateFlow()
    @Synchronized fun update(change: (SecaSnapshot) -> SecaSnapshot) { mutable.value = change(mutable.value) }
}

class SecaSettings(context: Context) {
    private val prefs = context.getSharedPreferences("seca_collection", Context.MODE_PRIVATE)
    val desired get() = prefs.getBoolean("desired", false)
    val address get() = prefs.getString("address", null)
    val rate get() = prefs.getInt("rate", 4000)
    fun start(address: String, rate: Int) {
        require(rate in SecaProtocol.rates)
        check(prefs.edit().putString("address", address).putInt("rate", rate).putBoolean("desired", true).commit()) { "Cannot persist seca recording settings" }
    }
    fun stop() { check(prefs.edit().putBoolean("desired", false).commit()) { "Cannot persist seca stop" } }
}

class SecaUploader(
    private val queue: SecaQueue,
    private val http: HttpJson,
    private val credentials: DeviceCredentialProvider,
    private val activeSession: () -> String? = { SecaStatus.updates.value.let { if (it.running) it.session else null } },
) {
    private val mutex = Mutex()
    private val schedule = SecaUploadSchedule()
    suspend fun drain() = mutex.withLock {
        while (true) {
            val pending = queue.pending()
            SecaStatus.update { it.copy(queued = pending.size) }
            val selected = schedule.select(pending, activeSession()) ?: break
            val file = selected.file
            val credential = credentials.currentCredential() ?: throw IOException("Connect this phone to Carolina to upload seca recordings")
            val body = file.readText(Charsets.UTF_8)
            val result = http.post("/api/v1/seca/batches", body, "Device $credential").requireSuccess()
            val ack = http.jsonObject(result.body)
            queue.acknowledge(file, HttpJson.requiredUuid(ack, "session_id"), HttpJson.requiredLong(ack, "batch_index"), HttpJson.requiredString(ack, "sha256"))
            schedule.acknowledged(selected)
            SecaStatus.update { it.copy(uploaded = it.uploaded + 1, queued = (pending.size - 1).coerceAtLeast(0), uploadError = null) }
            delay(30)
        }
    }
}

class SecaUploadWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result = try {
        (applicationContext as SensorsApplication).graph.secaUploader.drain()
        Result.success()
    } catch (cancelled: CancellationException) { throw cancelled
    } catch (error: Exception) {
        SecaStatus.update { it.copy(uploadError = error.message ?: "seca upload failed; saved on phone") }
        Result.retry()
    }
    companion object {
        fun enqueue(context: Context, freshAfterStop: Boolean = false) {
            WorkManager.getInstance(context).enqueueUniqueWork(
                // A newly sealed Stop must not wait behind an earlier network retry.
                // Startup/boot retain the chain: process creation must not cancel its worker.
                "durable_seca_upload", if (freshAfterStop) ExistingWorkPolicy.REPLACE else ExistingWorkPolicy.APPEND_OR_REPLACE,
                OneTimeWorkRequestBuilder<SecaUploadWorker>()
                    .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                    .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.SECONDS).build(),
            )
        }
    }
}

fun syncSecaDirectory(directory: File) {
    require(directory.isDirectory) { "seca sync target is not a directory" }
    val descriptor = Os.open(directory.absolutePath, OsConstants.O_RDONLY, 0)
    try { SecaTiming.measure("directory.fsync") { Os.fsync(descriptor) } } finally { Os.close(descriptor) }
}
