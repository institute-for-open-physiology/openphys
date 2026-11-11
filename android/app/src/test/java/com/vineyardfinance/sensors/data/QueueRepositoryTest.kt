package com.vineyardfinance.sensors.data

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.collector.H10Identity
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class QueueRepositoryTest {
    private lateinit var database: AppDatabase
    private lateinit var repository: QueueRepository
    private lateinit var context: Context
    private val capture =
        CaptureContext(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            epochStartElapsedNs = 0,
        )

    @Before
    fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        context
            .getSharedPreferences("installation_identity", Context.MODE_PRIVATE)
            .edit()
            .clear()
            .commit()
        database =
            Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
                .allowMainThreadQueries()
                .build()
        repository = QueueRepository(database, AppIdentity(context), clockMs = { 1234 })
    }

    @After
    fun tearDown() {
        database.close()
    }

    @Test
    fun eventsShareTheCurrentEpochAndSurviveQueueRestart() = runTest {
        repository.append(capture, record("one"))
        val event = repository.saveEvent("Walk", 10.0)
        repository.append(capture, record("two"))
        val batch = repository.nextBatch()!!
        assertEquals(3, batch.recordCount)
        assertTrue(batch.bodyJson.contains("manual_marker"))
        repository.acknowledge(batch, batch.lastSourceSeq)
        repository = QueueRepository(database, AppIdentity(context))
        repository.saveEvent("Stairs", 20.0, event.id)
        assertEquals(listOf(event.copy(label = "Stairs")), repository.readEvents())
        assertEquals(capture.writerEpochId, repository.nextBatch()!!.writerEpochId)
    }

    @Test
    fun eventEditsRollBackIfTheQueueWriteFails() = runTest {
        repository.append(capture, record("start"))
        val event = repository.saveEvent("Walk", 10.0)
        database.openHelper.writableDatabase.execSQL(
            "CREATE TRIGGER fail_record BEFORE INSERT ON pending_records BEGIN SELECT RAISE(ABORT, 'disk error'); END"
        )
        for (edit in
            listOf<suspend () -> Unit>(
                { repository.saveEvent("Another", 20.0) },
                { repository.saveEvent("Stairs", 20.0, event.id) },
                { repository.deleteEvent(event.id) },
            )) {
            assertTrue(runCatching { edit() }.isFailure)
            assertEquals(listOf(event), repository.readEvents())
            assertEquals(2L, repository.backlog().records)
        }
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER fail_record")
        repository.deleteEvent(event.id)
        repository.deleteEvent(event.id)
        assertTrue(repository.readEvents().isEmpty())
        assertTrue(repository.nextBatch()!!.bodyJson.contains("manual_marker_deleted"))
    }

    @Test
    fun eventsStartAFreshEpochAfterReboot() = runTest {
        repository = QueueRepository(database, AppIdentity(context), bootCount = 1)
        repository.append(capture, record("start"))
        val event = repository.saveEvent("Walk", 10.0)
        repository = QueueRepository(database, AppIdentity(context), bootCount = 2)
        repository.saveEvent("Stairs", 20.0, event.id)
        val before = repository.nextBatch()!!
        repository.acknowledge(before, before.lastSourceSeq)
        val after = repository.nextBatch()!!
        assertNotEquals(capture.writerEpochId, after.writerEpochId)
        assertEquals(1, after.recordCount)
        val records =
            com.google.gson.JsonParser.parseString(after.bodyJson)
                .asJsonObject["records"]
                .asJsonArray
        assertEquals(0L, records[0].asJsonObject["collector_monotonic_ns"].asLong)
    }

    @Test
    fun interleavedEpochsProduceContiguousBatches() = runTest {
        val other = capture.copy(writerEpochId = UUID.randomUUID().toString())
        repository.append(capture, record("one"))
        repository.append(other, record("two"))
        repository.append(capture, record("three"))
        repeat(3) { index ->
            val batch = repository.nextBatch()!!
            assertEquals(index.toLong(), batch.firstSourceSeq)
            assertEquals(1, batch.recordCount)
            repository.acknowledge(batch, batch.lastSourceSeq)
        }
        assertNull(repository.nextBatch())
    }

    @Test
    fun migrationPreservesExistingQueueState() = runTest {
        val name = "migration-test.sqlite3"
        context.deleteDatabase(name)
        val schema =
            com.google.gson.JsonParser.parseReader(
                    javaClass
                        .getResourceAsStream(
                            "/com.vineyardfinance.sensors.data.AppDatabase/1.json"
                        )!!
                        .reader()
                )
                .asJsonObject["database"]
                .asJsonObject
        context.openOrCreateDatabase(name, Context.MODE_PRIVATE, null).use { old ->
            schema["entities"].asJsonArray.forEach { item ->
                val entity = item.asJsonObject
                old.execSQL(
                    entity["createSql"]
                        .asString
                        .replace("\${TABLE_NAME}", entity["tableName"].asString)
                )
                entity["indices"].asJsonArray.forEach { index ->
                    old.execSQL(
                        index.asJsonObject["createSql"]
                            .asString
                            .replace("\${TABLE_NAME}", entity["tableName"].asString)
                    )
                }
            }
            old.execSQL("INSERT INTO counters VALUES ('source_sequence', 41)")
            old.execSQL("INSERT INTO runtime_state VALUES ('test', 'preserved')")
            old.version = 1
        }
        val migrated = AppDatabase.build(context, name)
        try {
            val queue = QueueRepository(migrated, AppIdentity(context))
            assertEquals(42L, queue.append(capture, record("after upgrade")))
            assertEquals("preserved", migrated.queueDao().state("test")!!.value)
            val event = queue.saveEvent("Walk", 10.0)
            assertEquals(listOf(event), queue.readEvents())
        } finally {
            migrated.close()
            context.deleteDatabase(name)
        }
    }

    @Test
    fun sourceSequencePersistsAndBatchBytesAreStable() = runTest {
        assertEquals(0, repository.append(capture, record("one")))
        assertEquals(1, repository.append(capture, record("two")))
        val first = repository.nextBatch()!!
        val retried = repository.nextBatch()!!
        assertEquals(0, first.firstSourceSeq)
        assertEquals(1, first.lastSourceSeq)
        assertEquals(first.batchId, retried.batchId)
        assertEquals(first.bodyJson, retried.bodyJson)
        assertEquals(first.bodySha256, QueueRepository.sha256(first.bodyJson.toByteArray()))
        assertTrue(first.bodyJson.contains("\"source\":\"chest_h10\""))
    }

    @Test
    fun preservesQueuedRecordJsonWithoutReencoding() = runTest {
        val recordJson = "{ \"record_id\" : \"preserved\", \"source_seq\" : 0 }"
        database.queueDao().insertRecord(
            PendingRecordEntity(
                sourceSeq = 0,
                recordId = "preserved",
                captureSessionId = capture.captureSessionId,
                writerEpochId = capture.writerEpochId,
                connectionId = null,
                collectorUnixNs = 1,
                collectorMonotonicNs = 2,
                kind = "collector_event",
                recordJson = recordJson,
                encodedBytes = recordJson.toByteArray().size,
            ),
        )

        assertTrue(repository.nextBatch()!!.bodyJson.contains("\"records\":[$recordJson]"))
    }

    @Test
    fun batchNeverCrossesWriterEpochAndAckDeletesOnlyCoveredData() = runTest {
        repository.append(capture, record("one"))
        val other =
            capture.copy(
                captureSessionId = UUID.randomUUID().toString(),
                writerEpochId = UUID.randomUUID().toString(),
            )
        repository.append(other, record("two"))
        val first = repository.nextBatch()!!
        assertEquals(1, first.recordCount)
        repository.acknowledge(first, first.lastSourceSeq)
        val second = repository.nextBatch()!!
        assertNotEquals(first.batchId, second.batchId)
        assertEquals(1, second.firstSourceSeq)
        repository.acknowledge(second, second.lastSourceSeq)
        assertNull(repository.nextBatch())
        assertEquals(0, repository.backlog().records)
    }

    @Test fun interleavedPhoneAndH10WritersDrainInOrderWithoutLosingProvenance() = runTest {
        val phone = capture.copy(captureSessionId = UUID.randomUUID().toString(), writerEpochId = UUID.randomUUID().toString())
        val contexts = listOf(phone, capture, phone, phone, capture)
        contexts.forEachIndexed { index, owner -> repository.append(owner, record("record-$index")) }
        val expected = listOf(0L..0L, 1L..1L, 2L..3L, 4L..4L)
        expected.forEach { range ->
            val batch = repository.nextBatch()!!
            assertEquals(range.first, batch.firstSourceSeq)
            assertEquals(range.last, batch.lastSourceSeq)
            assertEquals(contexts[range.first.toInt()].captureSessionId, batch.captureSessionId)
            assertEquals(contexts[range.first.toInt()].writerEpochId, batch.writerEpochId)
            assertEquals(batch.bodyJson, repository.nextBatch()!!.bodyJson)
            repository.acknowledge(batch, batch.lastSourceSeq)
        }
        assertNull(repository.nextBatch())
        assertEquals(0, repository.backlog().records)
    }

    @Test(expected = IllegalArgumentException::class)
    fun refusesAckThatDoesNotCoverBatch() = runTest {
        repository.append(capture, record("one"))
        val batch = repository.nextBatch()!!
        repository.acknowledge(batch, batch.lastSourceSeq - 1)
    }

    @Test
    fun capsBatchAt256Records() = runTest {
        repeat(300) { repository.append(capture, record("record-$it")) }
        assertEquals(256, repository.nextBatch()!!.recordCount)
    }

    @Test
    fun nonDefaultH10ProvenanceSurvivesDurableQueueAndBatchEncoding() = runTest {
        val nonDefault = capture.copy(physicalDeviceId = H10Identity.physicalDeviceId("A1B2C3D4"))
        repository.append(
            nonDefault,
            record("other-user-h10").copy(physicalDeviceId = nonDefault.physicalDeviceId),
        )
        val body = repository.nextBatch()!!.bodyJson
        assertTrue(body.contains("\"physical_device_id\":\"h10_a1b2c3d4\""))
        assertTrue(!body.contains("h10_0a0b0c0d"))
    }

    @Test fun backlogMatchesAFullScanThroughAppendAndAck() = runTest {
        repeat(300) { repository.append(capture, record("record-$it")) }
        assertEquals(scan(), repository.backlog())
        while (true) {
            val batch = repository.nextBatch() ?: break
            repository.acknowledge(batch, batch.lastSourceSeq)
            assertEquals(scan(), repository.backlog())
        }
        assertEquals(BacklogStats(0, null), repository.backlog())
    }

    @Test fun backlogQueryNeverScansPendingRecords() {
        val plan = database.openHelper.readableDatabase.query("EXPLAIN QUERY PLAN ${QueueDao.BACKLOG}").use { cursor ->
            generateSequence { if (cursor.moveToNext()) cursor.getString(3) else null }.toList()
        }
        println("backlog plan: $plan")
        assertTrue(plan.toString(), plan.none { it.startsWith("SCAN pending_records") })
    }

    private fun scan(): BacklogStats =
        database.openHelper.readableDatabase.query(
            "SELECT COUNT(*), MIN(collectorUnixNs) FROM pending_records",
        ).use { cursor ->
            cursor.moveToFirst()
            BacklogStats(cursor.getLong(0), if (cursor.isNull(1)) null else cursor.getLong(1))
        }

    private fun record(event: String) =
        RawRecord(
            connectionId = null,
            collectorUnixNs = 1,
            collectorMonotonicNs = 2,
            physicalDeviceId = "h10_test",
            blePeripheralId = null,
            kind = "collector_event",
            body = JsonObject().apply { addProperty("event", event) },
        )
}
