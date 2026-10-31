package com.vineyardfinance.sensors.network

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.auth.DeviceCredentialStore
import com.vineyardfinance.sensors.data.AppDatabase
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.data.QueueRepository
import com.vineyardfinance.sensors.data.RawRecord
import com.vineyardfinance.sensors.data.UploadBatchEntity
import com.vineyardfinance.sensors.data.UploadQueue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class IngestUploaderTest {
    private lateinit var server: MockWebServer
    private lateinit var database: AppDatabase
    private lateinit var repository: QueueRepository
    private lateinit var identity: AppIdentity
    private lateinit var credentials: FakeCredentialStore
    private lateinit var uploader: IngestUploader
    private val capture = CaptureContext(UUID.randomUUID().toString(), UUID.randomUUID().toString())

    @Before fun setUp() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        context.getSharedPreferences("installation_identity", Context.MODE_PRIVATE).edit().clear().commit()
        identity = AppIdentity(context)
        database = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).allowMainThreadQueries().build()
        repository = QueueRepository(database, identity)
        server = MockWebServer().apply { start() }
        credentials = FakeCredentialStore("access")
        uploader = IngestUploader(HttpJson(server.url("/").toString().trimEnd('/')), credentials, repository)
    }

    @After fun tearDown() {
        server.shutdown()
        database.close()
    }

    @Test fun uploadsExactStoredBatchWithDeviceCredentialAndDeletesAfterDurableAck() = runTest {
        repository.append(capture, record())
        val expected = repository.nextBatch()!!
        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(expected)))

        val result = uploader.drainUntilEmpty()

        assertTrue(result.complete)
        assertEquals(1, result.uploadedRecords)
        val request = server.takeRequest()
        assertEquals(IngestUploader.INGEST_PATH, request.path)
        assertEquals("Device access", request.getHeader("Authorization"))
        assertEquals(expected.bodyJson, request.body.readUtf8())
        assertEquals(0, repository.backlog().records)
    }

    @Test fun retriesSameBytesAfterAmbiguousServerFailure() = runTest {
        repository.append(capture, record())
        server.enqueue(MockResponse().setResponseCode(503))
        assertFalse(uploader.drainUntilEmpty().complete)
        val first = server.takeRequest().body.readUtf8()
        val batch = repository.nextBatch()!!
        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch)))
        assertTrue(uploader.drainUntilEmpty().complete)
        assertEquals(first, server.takeRequest().body.readUtf8())
    }

    @Test fun exactReconnectCodeOn401Or403ClearsCurrentCredentialAndRetainsQueue() = runTest {
        repository.append(capture, record())

        listOf(401, 403).forEach { status ->
            credentials.storeCredential("credential-$status")
            server.enqueue(
                MockResponse().setResponseCode(status)
                    .setBody("{\"code\":\"${IngestUploader.RECONNECT_REQUIRED_CODE}\"}"),
            )

            val result = uploader.drainUntilEmpty()

            assertFalse(result.complete)
            assertTrue(result.reconnectRequired)
            assertNull(credentials.currentCredential())
            assertEquals(1, repository.backlog().records)
            assertEquals("Device credential-$status", server.takeRequest().getHeader("Authorization"))
        }
    }

    @Test fun otherResponsesNeverClearCredentialEvenIfStatusOrBodyLooksSimilar() = runTest {
        repository.append(capture, record())
        val cases = listOf(
            401 to "{\"code\":\"invalid_request\"}",
            403 to "not-json",
            500 to "{\"code\":\"${IngestUploader.RECONNECT_REQUIRED_CODE}\"}",
        )

        cases.forEachIndexed { index, (status, body) ->
            val credential = "survives-$index"
            credentials.storeCredential(credential)
            server.enqueue(MockResponse().setResponseCode(status).setBody(body))

            val result = uploader.drainUntilEmpty()

            assertFalse(result.complete)
            assertFalse(result.reconnectRequired)
            assertEquals(credential, credentials.currentCredential())
            assertEquals(1, repository.backlog().records)
            server.takeRequest()
        }
    }

    @Test fun staleRejectedRequestCannotClearReplacementCredential() = runTest {
        repository.append(capture, record())
        val batch = repository.nextBatch()!!
        credentials.beforeCompareAndClear = { it.storeCredential("replacement") }
        server.enqueue(
            MockResponse().setResponseCode(401)
                .setBody("{\"code\":\"${IngestUploader.RECONNECT_REQUIRED_CODE}\"}"),
        )

        val rejected = uploader.drainUntilEmpty()

        assertFalse(rejected.complete)
        assertFalse(rejected.reconnectRequired)
        assertEquals("replacement", credentials.currentCredential())
        assertEquals(1, repository.backlog().records)
        assertEquals("Device access", server.takeRequest().getHeader("Authorization"))

        credentials.beforeCompareAndClear = null
        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch)))
        assertTrue(uploader.drainUntilEmpty().complete)
        assertEquals("Device replacement", server.takeRequest().getHeader("Authorization"))
        assertEquals(0, repository.backlog().records)
    }

    @Test fun missingCredentialPausesLocallyWithoutNetworkOrQueueLoss() = runTest {
        repository.append(capture, record())
        credentials.clear()

        val result = uploader.drainUntilEmpty()

        assertFalse(result.complete)
        assertTrue(result.reconnectRequired)
        assertEquals(0, server.requestCount)
        assertEquals(1, repository.backlog().records)
    }

    @Test fun retryLoopIdlesPromptlyWhenQueueIsEmpty() = runTest {
        var activeChecks = 0

        uploader.retryLoop(active = { ++activeChecks == 1 })

        assertEquals(2, activeChecks)
        assertEquals(0, server.requestCount)
    }

    @Test fun retryLoopWaitsLocallyForReconnectWithoutNetworkChurn() = runTest {
        repository.append(capture, record())
        credentials.clear()
        var activeChecks = 0

        uploader.retryLoop(active = { ++activeChecks <= 2 })

        assertTrue(activeChecks >= 3)
        assertEquals(0, server.requestCount)
        assertEquals(1, repository.backlog().records)
    }

    @Test fun retryLoopBacksOffAfterTransientServerFailure() = runTest {
        repository.append(capture, record())
        server.enqueue(MockResponse().setResponseCode(503))
        var activeChecks = 0

        uploader.retryLoop(active = { ++activeChecks == 1 })

        assertEquals(2, activeChecks)
        assertEquals(1, server.requestCount)
        assertEquals(1, repository.backlog().records)
    }

    @Test fun cancellationFromStatusCallbackEscapesWithoutNetworkOrQueueLoss() = runTest {
        repository.append(capture, record())

        val failure = runCatching {
            uploader.drainUntilEmpty { throw CancellationException("stop") }
        }.exceptionOrNull()

        assertTrue(failure is CancellationException)
        assertEquals(0, server.requestCount)
        assertEquals(1, repository.backlog().records)
    }

    @Test fun drainPacesBetweenSuccessfulBatchUploads() = runTest {
        // Two capture epochs force two batches in a single drain, pinning the delay
        // between consecutive successful uploads (the second batch is only created
        // mid-drain, so the server computes matching ACKs from each request body).
        repository.append(capture, record())
        val secondEpoch = CaptureContext(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        repository.append(secondEpoch, record())
        server.dispatcher = ackFromRequestDispatcher()
        val start = testScheduler.currentTime

        val result = uploader.drainUntilEmpty()

        assertTrue(result.complete)
        assertEquals(2, result.uploadedRecords)
        assertEquals(2, server.requestCount)
        assertEquals(2 * IngestUploader.INTER_BATCH_DELAY_MS, testScheduler.currentTime - start)
        assertEquals(0, repository.backlog().records)
    }

    private fun ackFromRequestDispatcher() = object : okhttp3.mockwebserver.Dispatcher() {
        override fun dispatch(request: okhttp3.mockwebserver.RecordedRequest): MockResponse {
            val bytes = request.body.readByteArray()
            val checksum = java.security.MessageDigest.getInstance("SHA-256")
                .digest(bytes)
                .joinToString("") { "%02x".format(it) }
            val json = com.google.gson.JsonParser
                .parseString(String(bytes, Charsets.UTF_8))
                .asJsonObject
            val recordCount = json.getAsJsonArray("records").size()
            return MockResponse().setResponseCode(200).setBody(
                "{\"schema_version\":1" +
                    ",\"installation_id\":\"${json.get("installation_id").asString}\"" +
                    ",\"batch_id\":\"${json.get("batch_id").asString}\"" +
                    ",\"batch_checksum_sha256\":\"$checksum\"" +
                    ",\"encoded_bytes\":${bytes.size}" +
                    ",\"record_count\":$recordCount" +
                    ",\"accepted_records\":$recordCount" +
                    ",\"durably_committed_through_seq\":${json.get("last_source_seq").asLong}" +
                    ",\"missing_ranges\":[],\"server_receive_unix_ms\":1}",
            )
        }
    }

    @Test fun vitalsTrackAttemptFailureAndDurableAck() = runTest {
        repository.append(capture, record())
        server.enqueue(MockResponse().setResponseCode(503))
        assertFalse(uploader.drainUntilEmpty().complete)
        var snapshot = uploader.vitals.snapshot()
        assertTrue(snapshot.attemptStartedAtMs > 0)
        assertEquals(1, snapshot.consecutiveFailures)
        assertTrue(snapshot.lastOutcome != null)
        assertEquals(0, snapshot.lastAckAtMs)

        val batch = repository.nextBatch()!!
        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch)))
        assertTrue(uploader.drainUntilEmpty().complete)
        snapshot = uploader.vitals.snapshot()
        assertEquals(0, snapshot.consecutiveFailures)
        assertTrue(snapshot.lastAckAtMs > 0)
        assertEquals("durable ack", snapshot.lastOutcome)
    }

    @Test fun retryLoopMarksReconnectWaitInVitals() = runTest {
        repository.append(capture, record())
        credentials.clear()
        var during: UploadLoopVitals.Snapshot? = null
        var activeChecks = 0

        uploader.retryLoop(active = {
            val check = ++activeChecks
            if (check == 2) during = uploader.vitals.snapshot()
            check <= 2
        })

        assertTrue(during!!.waitingForReconnect)
        val after = uploader.vitals.snapshot()
        assertFalse(after.waitingForReconnect)
        assertTrue(after.loopLaunchedAtMs > 0)
    }

    @Test fun rejectsMismatchedAckAndKeepsQueue() = runTest {
        repository.append(capture, record())
        val batch = repository.nextBatch()!!
        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch, batchId = UUID.randomUUID().toString())))
        assertFalse(uploader.drainUntilEmpty().complete)
        assertEquals(1, repository.backlog().records)
    }

    @Test fun rejectsChecksumLengthCountAndSequenceMismatchesBeforeDeleting() = runTest {
        repository.append(capture, record())
        repository.append(capture, record())
        val batch = repository.nextBatch()!!

        suspend fun rejected(response: String) {
            server.enqueue(MockResponse().setResponseCode(200).setBody(response))
            assertFalse(uploader.drainUntilEmpty().complete)
            assertEquals(2, repository.backlog().records)
        }

        rejected(ack(batch, checksum = "0".repeat(64)))
        rejected(ack(batch, encodedBytes = batch.bodyJson.toByteArray().size + 1))
        rejected(ack(batch, recordCount = batch.recordCount + 1))
        rejected(ack(batch, through = batch.lastSourceSeq - 1))
        rejected(ack(batch, through = batch.lastSourceSeq + 1))
        rejected(ack(batch, accepted = batch.recordCount + 1))
        rejected(ack(batch, accepted = batch.recordCount - 1))
        rejected(ack(batch, schemaVersion = 2))

        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch)))
        assertTrue(uploader.drainUntilEmpty().complete)
        assertEquals(0, repository.backlog().records)
    }

    @Test fun transientQueueReadFailurePausesForRetryInsteadOfKillingUploader() = runTest {
        var status = ""
        val failingQueue = object : UploadQueue {
            override suspend fun nextBatch(): UploadBatchEntity? = throw java.io.IOException("database busy")
            override suspend fun recordAttempt(batch: UploadBatchEntity, error: String?) = Unit
            override suspend fun acknowledge(batch: UploadBatchEntity, committedThrough: Long) = Unit
        }
        val local = IngestUploader(
            http = HttpJson(server.url("/").toString().trimEnd('/')),
            credentials = credentials,
            queue = failingQueue,
        )
        val result = local.drainUntilEmpty { status = it }
        assertFalse(result.complete)
        assertTrue(result.error!!.contains("local queue read failed"))
        assertTrue(status.contains("Upload paused"))
        assertEquals(0, server.requestCount)
    }

    @Test fun secondaryAttemptAndStatusFailuresDoNotPreventLaterExactRetry() = runTest {
        repository.append(capture, record())
        val batch = repository.nextBatch()!!
        val secondaryFailureQueue = object : UploadQueue {
            override suspend fun nextBatch(): UploadBatchEntity? = repository.nextBatch()
            override suspend fun recordAttempt(batch: UploadBatchEntity, error: String?) {
                throw java.io.IOException("attempt telemetry database failure")
            }
            override suspend fun acknowledge(batch: UploadBatchEntity, committedThrough: Long) =
                repository.acknowledge(batch, committedThrough)
        }
        val local = IngestUploader(
            http = HttpJson(server.url("/").toString().trimEnd('/')),
            credentials = credentials,
            queue = secondaryFailureQueue,
        )
        val brokenStatus: suspend (String) -> Unit = { throw java.io.IOException("status telemetry failure") }

        server.enqueue(MockResponse().setResponseCode(503))
        assertFalse(local.drainUntilEmpty(brokenStatus).complete)
        assertEquals(1, repository.backlog().records)
        assertEquals(batch.bodyJson, server.takeRequest().body.readUtf8())

        server.enqueue(MockResponse().setResponseCode(200).setBody(ack(batch)))
        assertTrue(local.drainUntilEmpty(brokenStatus).complete)
        assertEquals(batch.bodyJson, server.takeRequest().body.readUtf8())
        assertEquals(0, repository.backlog().records)
    }

    private fun ack(
        batch: UploadBatchEntity,
        batchId: String = batch.batchId,
        through: Long = batch.lastSourceSeq,
        accepted: Int = batch.recordCount,
        checksum: String = batch.bodySha256,
        encodedBytes: Int = batch.bodyJson.toByteArray(Charsets.UTF_8).size,
        recordCount: Int = batch.recordCount,
        schemaVersion: Int = 1,
    ) = "{\"schema_version\":$schemaVersion,\"installation_id\":\"${identity.installationId}\",\"batch_id\":\"$batchId\",\"batch_checksum_sha256\":\"$checksum\",\"encoded_bytes\":$encodedBytes,\"record_count\":$recordCount,\"accepted_records\":$accepted,\"durably_committed_through_seq\":$through,\"missing_ranges\":[],\"server_receive_unix_ms\":1}"

    private fun record() = RawRecord(
        connectionId = null,
        collectorUnixNs = 1,
        collectorMonotonicNs = 2,
        physicalDeviceId = "h10_test",
        blePeripheralId = null,
        kind = "collector_event",
        body = JsonObject().apply { addProperty("event", "test") },
    )

    private class FakeCredentialStore(initial: String? = null) : DeviceCredentialStore {
        private var value = initial
        private val mutable = MutableStateFlow(initial != null)
        var beforeCompareAndClear: ((FakeCredentialStore) -> Unit)? = null
        override val connected: StateFlow<Boolean> = mutable
        override fun currentCredential(): String? = value
        override fun storeCredential(credential: String) {
            value = credential
            mutable.value = true
        }
        override fun clearIfCurrent(rejectedCredential: String): Boolean {
            beforeCompareAndClear?.invoke(this)
            if (value != rejectedCredential) return false
            clear()
            return true
        }
        override fun clear() {
            value = null
            mutable.value = false
        }
    }
}
