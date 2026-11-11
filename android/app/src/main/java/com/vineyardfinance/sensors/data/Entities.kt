package com.vineyardfinance.sensors.data

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(tableName = "counters")
data class CounterEntity(@PrimaryKey val name: String, val value: Long)

@Entity(
    tableName = "pending_records",
    indices = [Index("batchId"), Index(value = ["captureSessionId", "sourceSeq"])],
)
data class PendingRecordEntity(
    @PrimaryKey val sourceSeq: Long,
    val recordId: String,
    val captureSessionId: String,
    val writerEpochId: String,
    val connectionId: String?,
    val collectorUnixNs: Long,
    val collectorMonotonicNs: Long,
    val kind: String,
    val recordJson: String,
    val encodedBytes: Int,
    val batchId: String? = null,
)

@Entity(tableName = "upload_batches", indices = [Index("firstSourceSeq")])
data class UploadBatchEntity(
    @PrimaryKey val batchId: String,
    val installationId: String,
    val captureSessionId: String,
    val writerEpochId: String,
    val firstSourceSeq: Long,
    val lastSourceSeq: Long,
    val recordCount: Int,
    val bodyJson: String,
    val bodySha256: String,
    val createdUnixMs: Long,
    val attempts: Int = 0,
    val lastAttemptUnixMs: Long? = null,
    val lastError: String? = null,
)

@Entity(tableName = "runtime_state")
data class RuntimeStateEntity(@PrimaryKey val key: String, val value: String)

data class BacklogStats(
    val records: Long,
    val oldestUnixNs: Long?,
)

@Entity(tableName = "events")
data class SensorEvent(@PrimaryKey val id: String, val time: Double, val label: String)
