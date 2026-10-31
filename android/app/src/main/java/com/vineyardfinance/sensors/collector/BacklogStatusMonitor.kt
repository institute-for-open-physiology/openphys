package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.data.BacklogStats
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive

/**
 * Keeps one process-lifetime subscription to the durable queue. The service cannot own this
 * subscription because its residual WorkManager drain deliberately continues after service stop.
 */
internal class BacklogStatusMonitor(
    private val backlog: Flow<BacklogStats>,
    private val publish: (BacklogStats) -> Unit,
    private val publishUnavailable: (String) -> Unit,
    private val waitBeforeRetry: suspend () -> Unit = { delay(RETRY_DELAY_MS) },
) {
    suspend fun run() {
        while (currentCoroutineContext().isActive) {
            val failure = try {
                backlog.collect { publish(it) }
                "Local queue status stream ended unexpectedly"
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                "Local queue status unavailable: ${safe(error)}"
            }
            publishUnavailable(failure)
            waitBeforeRetry()
        }
    }

    private fun safe(error: Exception): String =
        (error.message ?: error.javaClass.simpleName).take(MAX_ERROR_LENGTH)

    companion object {
        private const val RETRY_DELAY_MS = 1_000L
        private const val MAX_ERROR_LENGTH = 240
    }
}
