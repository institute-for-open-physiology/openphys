package com.vineyardfinance.sensors.seca

import com.vineyardfinance.sensors.auth.DeviceCredentialProvider
import com.vineyardfinance.sensors.network.HttpJson
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.util.UUID

class SecaUploaderTest {
    @get:Rule val temp = TemporaryFolder()

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test fun replacingCancelledUploadRetainsExactChunkAndReleasesQueueOwnership() = runTest {
        val server = MockWebServer(); server.start()
        try {
            val queue = SecaQueue(temp.newFolder()) {}
            val session = UUID.randomUUID().toString()
            val file = queue.begin(SecaBatch(installation_id = UUID.randomUUID().toString(),
                session_id = session, batch_index = 0, device_id = "test", sample_rate_hz = 2000)).finish("user_stop")
            val body = file.readText()
            val digest = SecaQueue.sha(file.readBytes())
            val credentials = object : DeviceCredentialProvider {
                override val connected = MutableStateFlow(true)
                override fun currentCredential() = "secret"
            }
            val uploader = SecaUploader(queue, HttpJson(server.url("/").toString()), credentials)
            // Simulate replacing a worker after its request reached the server but before ACK.
            server.enqueue(MockResponse().setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.NO_RESPONSE))
            val oldWorker = launch { uploader.drain() }
            runCurrent()
            val first = requireNotNull(server.takeRequest(2, java.util.concurrent.TimeUnit.SECONDS))
            assertEquals(body, first.body.readUtf8())
            oldWorker.cancelAndJoin()
            assertTrue(file.isFile); assertEquals(body, file.readText())

            server.enqueue(MockResponse().setBody("{\"session_id\":\"$session\",\"batch_index\":0,\"sha256\":\"$digest\"}"))
            uploader.drain() // The cancelled worker must have released this same uploader's mutex.
            val retried = requireNotNull(server.takeRequest(2, java.util.concurrent.TimeUnit.SECONDS))
            assertEquals(body, retried.body.readUtf8())
            assertFalse(file.exists()); assertTrue(queue.pending().isEmpty())
        } finally { server.shutdown() }
    }

    @Test fun retryIsByteIdenticalAndOnlyExactHashAcknowledgementDeletes() = runTest {
        val server = MockWebServer(); server.start()
        try {
            val queue = SecaQueue(temp.newFolder()) {}
            val session = UUID.randomUUID().toString()
            val header = SecaBatch(installation_id = UUID.randomUUID().toString(), session_id = session, batch_index = 0, device_id = "test", sample_rate_hz = 4000)
            val file = queue.begin(header).finish("user_stop")
            val body = file.readText()
            var credential: String? = "secret"
            val credentials = object : DeviceCredentialProvider {
                override val connected = MutableStateFlow(true)
                override fun currentCredential() = credential
            }
            val uploader = SecaUploader(queue, HttpJson(server.url("/").toString()), credentials)
            server.enqueue(MockResponse().setResponseCode(503))
            assertTrue(runCatching { uploader.drain() }.isFailure)
            val first = server.takeRequest()
            assertEquals("/api/v1/seca/batches", first.path); assertEquals("Device secret", first.getHeader("Authorization"))
            assertEquals(body, first.body.readUtf8()); assertTrue(file.isFile)
            server.enqueue(MockResponse().setBody("{\"session_id\":\"$session\",\"batch_index\":0,\"sha256\":\"wrong\"}"))
            assertTrue(runCatching { uploader.drain() }.isFailure)
            assertEquals(body, server.takeRequest().body.readUtf8()); assertTrue(file.isFile)
            credential = null
            assertTrue(runCatching { uploader.drain() }.isFailure); assertTrue(file.isFile)
            credential = "secret"
            server.enqueue(MockResponse().setBody("{\"session_id\":\"$session\",\"batch_index\":0,\"sha256\":\"${SecaQueue.sha(file.readBytes())}\"}"))
            uploader.drain()
            assertEquals(body, server.takeRequest().body.readUtf8()); assertFalse(file.exists())
            uploader.drain()
            assertEquals(0, SecaStatus.updates.value.queued)
        } finally { server.shutdown() }
    }
    @Test fun activeRecordingAdvancesWhileOldSessionsDrainAndFailedAckKeepsTurn() = runTest {
        val server = MockWebServer(); server.start()
        try {
            val queue = SecaQueue(temp.newFolder()) {}
            val old = "00000000-0000-0000-0000-000000000001"
            val active = "ffffffff-ffff-ffff-ffff-ffffffffffff"
            val installation = UUID.randomUUID().toString()
            val files = mutableMapOf<Pair<String, Int>, java.io.File>()
            for ((session, count) in listOf(old to 2, active to 4)) repeat(count) { index ->
                files[session to index] = queue.begin(SecaBatch(installation_id = installation, session_id = session,
                    batch_index = index.toLong(), device_id = "test", sample_rate_hz = 2000)).finish("user_stop")
            }
            val credentials = object : DeviceCredentialProvider {
                override val connected = MutableStateFlow(true)
                override fun currentCredential() = "secret"
            }
            val uploader = SecaUploader(queue, HttpJson(server.url("/").toString()), credentials) { active }
            val expected = listOf(active to 0, active to 1, active to 2, old to 0, active to 3, old to 1)
            fun ack(key: Pair<String, Int>) = MockResponse().setBody("{\"session_id\":\"${key.first}\",\"batch_index\":${key.second},\"sha256\":\"${SecaQueue.sha(files.getValue(key).readBytes())}\"}")
            val bodies = expected.map { files.getValue(it).readText() }
            expected.take(3).forEach { server.enqueue(ack(it)) }
            server.enqueue(MockResponse().setResponseCode(503))
            assertTrue(runCatching { uploader.drain() }.isFailure)
            repeat(4) { assertEquals(bodies[it], server.takeRequest().body.readUtf8()) }
            assertEquals(3, queue.pending().size)
            expected.drop(3).forEach { server.enqueue(ack(it)) }
            uploader.drain()
            repeat(3) { assertEquals(bodies[it + 3], server.takeRequest().body.readUtf8()) }
            assertTrue(queue.pending().isEmpty())
        } finally { server.shutdown() }
    }

    @Test fun directorySyncFailureAfterValidAckDoesNotAdvanceFairnessBudget() = runTest {
        val server = MockWebServer(); server.start()
        try {
            var failSync = false
            val queue = SecaQueue(temp.newFolder()) { if (failSync) throw java.io.IOException("directory sync failed") }
            val old = "00000000-0000-0000-0000-000000000001"
            val active = "ffffffff-ffff-ffff-ffff-ffffffffffff"
            val installation = UUID.randomUUID().toString()
            val files = mutableMapOf<Pair<String, Int>, java.io.File>()
            for ((session, count) in listOf(old to 2, active to 4)) repeat(count) { index ->
                files[session to index] = queue.begin(SecaBatch(installation_id = installation, session_id = session,
                    batch_index = index.toLong(), device_id = "test", sample_rate_hz = 2000)).finish("user_stop")
            }
            val credentials = object : DeviceCredentialProvider {
                override val connected = MutableStateFlow(true)
                override fun currentCredential() = "secret"
            }
            val uploader = SecaUploader(queue, HttpJson(server.url("/").toString()), credentials) { active }
            fun ack(key: Pair<String, Int>) = MockResponse().setBody("{\"session_id\":\"${key.first}\",\"batch_index\":${key.second},\"sha256\":\"${SecaQueue.sha(files.getValue(key).readBytes())}\"}")
            repeat(3) { server.enqueue(ack(active to it)) }
            server.enqueue(MockResponse().setResponseCode(503))
            assertTrue(runCatching { uploader.drain() }.isFailure)
            repeat(4) { server.takeRequest() }
            server.enqueue(ack(old to 0)); failSync = true
            assertTrue(runCatching { uploader.drain() }.isFailure)
            server.takeRequest(); assertFalse(files.getValue(old to 0).exists())
            failSync = false
            val expected = listOf(old to 1, active to 3)
            val bodies = expected.map { files.getValue(it).readText() }
            expected.forEach { server.enqueue(ack(it)) }
            uploader.drain()
            repeat(2) { assertEquals(bodies[it], server.takeRequest().body.readUtf8()) }
        } finally { server.shutdown() }
    }

}
