package com.vineyardfinance.sensors.data

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Update
import kotlinx.coroutines.flow.Flow

@Dao
interface QueueDao {
    @Query("SELECT * FROM events ORDER BY time DESC")
    suspend fun readEvents(): List<SensorEvent>

    @Query("SELECT * FROM events WHERE id = :id")
    suspend fun event(id: String): SensorEvent?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun saveEvent(event: SensorEvent)

    @Query("DELETE FROM events WHERE id = :id")
    suspend fun deleteEvent(id: String)

    @Query("SELECT value FROM counters WHERE name = :name")
    suspend fun counter(name: String): Long?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun putCounter(counter: CounterEntity)

    @Insert
    suspend fun insertRecord(record: PendingRecordEntity)

    @Query("SELECT * FROM pending_records WHERE batchId IS NULL ORDER BY sourceSeq LIMIT 1")
    suspend fun firstUnbatched(): PendingRecordEntity?

    @Query("SELECT * FROM pending_records WHERE batchId IS NULL ORDER BY sourceSeq LIMIT :limit")
    suspend fun unbatchedInOrder(limit: Int): List<PendingRecordEntity>

    @Query("UPDATE pending_records SET batchId = :batchId WHERE sourceSeq BETWEEN :first AND :last AND batchId IS NULL")
    suspend fun assignBatch(batchId: String, first: Long, last: Long): Int

    @Insert
    suspend fun insertBatch(batch: UploadBatchEntity)

    @Query("SELECT * FROM upload_batches ORDER BY firstSourceSeq LIMIT 1")
    suspend fun oldestBatch(): UploadBatchEntity?

    @Update
    suspend fun updateBatch(batch: UploadBatchEntity)

    @Query("DELETE FROM pending_records WHERE sourceSeq <= :sequence")
    suspend fun deleteRecordsThrough(sequence: Long): Int

    @Query("DELETE FROM upload_batches WHERE lastSourceSeq <= :sequence")
    suspend fun deleteBatchesThrough(sequence: Long): Int

    @Query("SELECT COUNT(*) AS records, COALESCE(SUM(encodedBytes), 0) AS bytes, MIN(collectorUnixNs) AS oldestUnixNs FROM pending_records")
    fun observeBacklog(): Flow<BacklogStats>

    @Query("SELECT COUNT(*) AS records, COALESCE(SUM(encodedBytes), 0) AS bytes, MIN(collectorUnixNs) AS oldestUnixNs FROM pending_records")
    suspend fun backlog(): BacklogStats

    @Query("SELECT MAX(sourceSeq) FROM pending_records")
    suspend fun highestPendingSequence(): Long?

    @Query("SELECT * FROM runtime_state WHERE key = :key")
    suspend fun state(key: String): RuntimeStateEntity?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun putState(state: RuntimeStateEntity)
}
