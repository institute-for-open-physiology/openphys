package com.vineyardfinance.sensors.network

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.vineyardfinance.sensors.SensorsApplication
import java.util.concurrent.TimeUnit

class UploadWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val graph = (applicationContext as SensorsApplication).graph
        if (graph.credentials.currentCredential() == null) {
            return UploadWorkPolicy.decide(
                credentialAvailable = false,
                drain = null,
                runAttemptCount = runAttemptCount,
            ).asWorkerResult()
        }
        val drained = graph.uploader.drainUntilEmpty()
        return UploadWorkPolicy.decide(
            credentialAvailable = true,
            drain = drained,
            runAttemptCount = runAttemptCount,
        ).asWorkerResult()
    }

    private fun UploadWorkOutcome.asWorkerResult(): Result = when (this) {
        UploadWorkOutcome.SUCCESS -> Result.success()
        UploadWorkOutcome.RETRY -> Result.retry()
        UploadWorkOutcome.FAILURE -> Result.failure()
    }

    companion object {
        private const val UPLOAD_WORK = "durable_h10_upload"

        fun enqueue(context: Context) {
            WorkManager.getInstance(context).enqueueUniqueWork(
                UPLOAD_WORK,
                ExistingWorkPolicy.REPLACE,
                OneTimeWorkRequestBuilder<UploadWorker>()
                    .setConstraints(
                        Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build(),
                    )
                    .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.SECONDS)
                    .build(),
            )
        }
    }
}
