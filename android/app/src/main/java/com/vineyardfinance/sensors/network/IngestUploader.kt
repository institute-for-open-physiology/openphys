package com.vineyardfinance.sensors.network

import com.vineyardfinance.sensors.auth.DeviceCredentialStore
import com.vineyardfinance.sensors.data.UploadQueue
import com.vineyardfinance.sensors.data.UploadBatchEntity
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import java.io.IOException
import kotlin.math.min
import kotlin.random.Random

class IngestUploader(
    private val http: HttpJson,
    private val credentials: DeviceCredentialStore,
    private val queue: UploadQueue,
    private val random: Random = Random.Default,
    val vitals: UploadLoopVitals = UploadLoopVitals(),
    private val interBatchDelayMs: Long = INTER_BATCH_DELAY_MS,
) {
    suspend fun drainUntilEmpty(onStatus: suspend (String) -> Unit = {}): DrainResult {
        var uploaded = 0
        while (true) {
            val batch = try {
                queue.nextBatch()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                val message = "local queue read failed: ${safeError(error)}"
                vitals.attemptFailed(message)
                reportStatus(onStatus, "Upload paused: $message")
                return DrainResult(uploaded, false, message, reconnectRequired = false)
            } ?: return DrainResult(uploaded, true, null)
            try {
                upload(batch, onStatus)
                uploaded += batch.recordCount
                // Pace catch-up drains so a large backlog gives the radio and CPU
                // duty-cycle gaps instead of a sustained slam.
                delay(interBatchDelayMs)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                val message = safeError(error)
                vitals.attemptFailed(message)
                bestEffort { queue.recordAttempt(batch, message) }
                reportStatus(onStatus, "Upload paused: $message")
                return DrainResult(uploaded, false, message, error is DeviceReconnectRequired)
            }
        }
    }

    suspend fun retryLoop(active: () -> Boolean, onStatus: suspend (String) -> Unit = {}) {
        vitals.loopLaunched()
        var delayMs = 250L
        while (active()) {
            vitals.iteration()
            val result = try {
                drainUntilEmpty(onStatus)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                val message = "upload loop recovered: ${safeError(error)}"
                vitals.attemptFailed(message)
                reportStatus(onStatus, "Upload paused: $message")
                DrainResult(0, false, message)
            }
            if (result.complete) {
                delay(100)
                delayMs = 250L
            } else if (result.reconnectRequired) {
                vitals.reconnectWait(true)
                while (active() && credentials.currentCredential() == null) delay(RECONNECT_POLL_MS)
                vitals.reconnectWait(false)
                delayMs = 250L
            } else {
                // Backoff counts consecutive failures; an accepted batch restarts it.
                if (result.uploadedRecords > 0) delayMs = 250L
                vitals.backoff(delayMs)
                delay(delayMs + random.nextLong(0, 251))
                delayMs = min(delayMs * 2, MAX_BACKOFF_MS)
            }
        }
    }

    private suspend fun upload(batch: UploadBatchEntity, onStatus: suspend (String) -> Unit) {
        val credential = credentials.currentCredential() ?: throw DeviceReconnectRequired()
        vitals.attemptStarted()
        reportStatus(onStatus, "Uploading records ${batch.firstSourceSeq}–${batch.lastSourceSeq}")
        val response = http.post(INGEST_PATH, batch.bodyJson, "Device $credential")
        if ((response.code == 401 || response.code == 403) && terminalReconnectCode(response.body)) {
            if (credentials.clearIfCurrent(credential)) throw DeviceReconnectRequired()
            throw IOException("device credential changed while an earlier request was rejected; retrying")
        }
        response.requireSuccess()
        val json = http.jsonObject(response.body)
        require(HttpJson.requiredLong(json, "schema_version") == 1L) { "ACK schema_version mismatch" }
        require(HttpJson.requiredString(json, "installation_id") == batch.installationId) { "ACK installation mismatch" }
        require(HttpJson.requiredString(json, "batch_id") == batch.batchId) { "ACK batch mismatch" }
        require(HttpJson.requiredString(json, "batch_checksum_sha256") == batch.bodySha256) { "ACK checksum mismatch" }
        require(HttpJson.requiredLong(json, "encoded_bytes") == batch.bodyJson.toByteArray(Charsets.UTF_8).size.toLong()) { "ACK encoded_bytes mismatch" }
        require(HttpJson.requiredLong(json, "record_count") == batch.recordCount.toLong()) { "ACK record_count mismatch" }
        val accepted = HttpJson.requiredLong(json, "accepted_records")
        require(accepted == 0L || accepted == batch.recordCount.toLong()) { "ACK accepted_records invalid" }
        val committed = HttpJson.requiredLong(json, "durably_committed_through_seq")
        require(committed <= batch.lastSourceSeq) { "ACK exceeds uploaded batch sequence" }
        queue.acknowledge(batch, committed)
        vitals.ackReceived()
        reportStatus(onStatus, "Durable ACK through $committed")
    }

    private suspend fun reportStatus(onStatus: suspend (String) -> Unit, message: String) {
        bestEffort { onStatus(message) }
    }

    private suspend fun bestEffort(block: suspend () -> Unit) {
        try {
            block()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // Upload telemetry/attempt counters are secondary to retaining and retrying the
            // canonical queued batch. A later loop iteration retries the exact stored bytes.
        }
    }

    data class DrainResult(
        val uploadedRecords: Int,
        val complete: Boolean,
        val error: String?,
        val reconnectRequired: Boolean = false,
    )

    private class DeviceReconnectRequired : IOException("Reconnect this phone; collection continues locally")

    companion object {
        const val INGEST_PATH = "/api/v1/ingest/batches"
        const val RECONNECT_REQUIRED_CODE = "device_reconnect_required"
        const val INTER_BATCH_DELAY_MS = 150L
        const val MAX_BACKOFF_MS = 15 * 60_000L
        private const val RECONNECT_POLL_MS = 1_000L

        private fun terminalReconnectCode(body: String): Boolean = runCatching {
            HttpJson.requiredString(com.google.gson.JsonParser.parseString(body).asJsonObject, "code") == RECONNECT_REQUIRED_CODE
        }.getOrDefault(false)

        private fun safeError(error: Throwable): String = when (error) {
            is HttpJson.HttpStatusException -> error.message ?: "HTTP failure"
            is IOException -> error.message ?: "network failure"
            else -> error.message?.take(160) ?: error.javaClass.simpleName
        }
    }
}
