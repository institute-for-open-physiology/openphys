package com.vineyardfinance.sensors.data

import androidx.room.withTransaction
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import kotlinx.coroutines.flow.Flow
import java.security.MessageDigest
import java.util.UUID

interface UploadQueue {
    suspend fun nextBatch(): UploadBatchEntity?
    suspend fun recordAttempt(batch: UploadBatchEntity, error: String?)
    suspend fun acknowledge(batch: UploadBatchEntity, committedThrough: Long)
}

class QueueRepository(
    private val database: AppDatabase,
    private val identity: AppIdentity,
    private val clockMs: () -> Long = System::currentTimeMillis,
    private val bootCount: Int = identity.bootCount,
) : UploadQueue {
    private val dao = database.queueDao()
    private val gson: Gson = GsonBuilder().serializeNulls().disableHtmlEscaping().create()

    suspend fun append(context: CaptureContext, record: RawRecord): Long = database.withTransaction {
        val next = (dao.counter(SOURCE_SEQUENCE) ?: -1L) + 1L
        check(next >= 0) { "source sequence exhausted" }
        val json = RecordJson.encode(next, record)
        dao.insertRecord(
            PendingRecordEntity(
                sourceSeq = next,
                recordId = record.recordId,
                captureSessionId = context.captureSessionId,
                writerEpochId = context.writerEpochId,
                connectionId = record.connectionId,
                collectorUnixNs = record.collectorUnixNs,
                collectorMonotonicNs = record.collectorMonotonicNs,
                kind = record.kind,
                recordJson = json,
                encodedBytes = json.toByteArray().size,
            ),
        )
        dao.putCounter(CounterEntity(SOURCE_SEQUENCE, next))
        val saved = dao.state(LATEST_CAPTURE)?.value
        val captureJson = gson.toJson(context)
        if (saved != captureJson) dao.putState(RuntimeStateEntity(LATEST_CAPTURE, captureJson))
        if (dao.state(LATEST_BOOT)?.value != bootCount.toString())
            dao.putState(RuntimeStateEntity(LATEST_BOOT, bootCount.toString()))
        next
    }

    suspend fun readEvents(): List<SensorEvent> = dao.readEvents()

    suspend fun saveEvent(label: String, time: Double, id: String? = null): SensorEvent = database.withTransaction {
        val previous = id?.let { dao.event(it) ?: error("Event no longer exists") }
        val event = SensorEvent(id ?: UUID.randomUUID().toString(), previous?.time ?: time, label.trim().take(120).ifEmpty { "Event" })
        dao.saveEvent(event)
        appendEvent(event, if (previous == null) "manual_marker" else "manual_marker_updated")
        event
    }

    suspend fun deleteEvent(id: String) = database.withTransaction {
        val event = dao.event(id) ?: return@withTransaction
        dao.deleteEvent(id)
        appendEvent(event, "manual_marker_deleted")
    }

    private suspend fun appendEvent(event: SensorEvent, action: String) {
        val saved = dao.state(LATEST_CAPTURE)?.value ?: error("Connect the sensor before adding events")
        val previous = gson.fromJson(saved, CaptureContext::class.java)
        val context = if (dao.state(LATEST_BOOT)?.value == bootCount.toString()) previous else
            previous.copy(captureSessionId = UUID.randomUUID().toString(), writerEpochId = UUID.randomUUID().toString(),
                epochStartElapsedNs = android.os.SystemClock.elapsedRealtimeNanos(), connectionId = null)
        val body = JsonObject().apply {
            addProperty("event", action)
            addProperty("severity", "info")
            addProperty("detail", event.label)
            add("fields", JsonObject().apply {
                addProperty("id", event.id)
                addProperty("label", event.label)
                addProperty("marked_unix_ms", (event.time * 1000).toLong())
            })
        }
        append(context, RawRecord.now(context, "collector_event", body))
    }

    override suspend fun nextBatch(): UploadBatchEntity? = database.withTransaction {
        dao.oldestBatch() ?: createBatchLocked()
    }

    private suspend fun createBatchLocked(): UploadBatchEntity? {
        val first = dao.firstUnbatched() ?: return null
        val candidates = dao.unbatchedInOrder(MAX_RECORDS)
        val selected = ArrayList<PendingRecordEntity>(candidates.size)
        var bytes = 256
        for (record in candidates) {
            if (record.captureSessionId != first.captureSessionId || record.writerEpochId != first.writerEpochId) break
            require(selected.isEmpty() || record.sourceSeq == selected.last().sourceSeq + 1) {
                "non-contiguous local source sequence at ${record.sourceSeq}"
            }
            // Concurrent phone GPS and H10 writers may interleave source sequences.
            // Seal at their boundary; never skip another writer's earlier record.
            if (record.captureSessionId != first.captureSessionId || record.writerEpochId != first.writerEpochId) break
            val nextBytes = bytes + record.encodedBytes + 1
            if (selected.isNotEmpty() && nextBytes > MAX_BATCH_BYTES) break
            require(nextBytes <= MAX_BATCH_BYTES) { "single record exceeds batch size limit" }
            selected += record
            bytes = nextBytes
        }
        if (selected.isEmpty()) return null
        val batchId = UUID.randomUUID().toString()
        val body = JsonObject().apply {
            addProperty("schema_version", 1)
            addProperty("installation_id", identity.installationId)
            addProperty("batch_id", batchId)
            addProperty("source", "chest_h10")
            addProperty("capture_session_id", first.captureSessionId)
            addProperty("writer_epoch_id", first.writerEpochId)
            addProperty("first_source_seq", selected.first().sourceSeq)
            addProperty("last_source_seq", selected.last().sourceSeq)
            add("records", JsonArray().apply { selected.forEach { add(gson.fromJson(it.recordJson, JsonObject::class.java)) } })
        }
        val bodyJson = gson.toJson(body)
        require(bodyJson.toByteArray().size <= MAX_BATCH_BYTES) { "encoded batch exceeds size limit" }
        val batch = UploadBatchEntity(
            batchId = batchId,
            installationId = identity.installationId,
            captureSessionId = first.captureSessionId,
            writerEpochId = first.writerEpochId,
            firstSourceSeq = selected.first().sourceSeq,
            lastSourceSeq = selected.last().sourceSeq,
            recordCount = selected.size,
            bodyJson = bodyJson,
            bodySha256 = sha256(bodyJson.toByteArray()),
            createdUnixMs = clockMs(),
        )
        check(dao.assignBatch(batchId, batch.firstSourceSeq, batch.lastSourceSeq) == selected.size)
        dao.insertBatch(batch)
        return batch
    }

    override suspend fun recordAttempt(batch: UploadBatchEntity, error: String?) = database.withTransaction {
        dao.updateBatch(batch.copy(attempts = batch.attempts + 1, lastAttemptUnixMs = clockMs(), lastError = error))
    }

    override suspend fun acknowledge(batch: UploadBatchEntity, committedThrough: Long) = database.withTransaction {
        require(committedThrough >= batch.lastSourceSeq) { "server ACK did not cover uploaded batch" }
        val highest = dao.highestPendingSequence()
        require(highest == null || committedThrough <= highest) { "server ACK exceeds local sequence" }
        dao.deleteRecordsThrough(committedThrough)
        dao.deleteBatchesThrough(committedThrough)
        dao.putState(RuntimeStateEntity(LAST_ACK, committedThrough.toString()))
    }

    fun observeBacklog(): Flow<BacklogStats> = dao.observeBacklog()
    suspend fun backlog(): BacklogStats = dao.backlog()

    companion object {
        const val MAX_RECORDS = 256
        const val MAX_BATCH_BYTES = 512 * 1024
        private const val SOURCE_SEQUENCE = "source_sequence"
        private const val LAST_ACK = "last_durable_ack"
        private const val LATEST_BOOT = "latest_capture_boot"
        private const val LATEST_CAPTURE = "latest_capture_context"

        fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
            .digest(bytes).joinToString("") { "%02x".format(it) }
    }
}
