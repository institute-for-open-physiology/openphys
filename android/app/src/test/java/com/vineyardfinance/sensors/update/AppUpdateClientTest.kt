package com.vineyardfinance.sensors.update

import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.IOException
import java.security.MessageDigest

class AppUpdateClientTest {
    private lateinit var server: MockWebServer
    private lateinit var client: AppUpdateClient
    private lateinit var workDir: File

    @Before fun start() {
        server = MockWebServer()
        server.start(0)
        // MockWebServer serves plain HTTP on localhost, which the client's
        // HTTPS requirement explicitly allows for loopback.
        client = AppUpdateClient("http://localhost:${server.port}")
        workDir = createTempDir("app-update-test")
    }

    @After fun stop() {
        server.shutdown()
        workDir.deleteRecursively()
    }

    @Test fun previewParsesMetadataAndTicketCookieAndSendsTokenInBodyOnly() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .addHeader(
                    "Set-Cookie",
                    "sensors_apk_ticket=ticket-value; Path=/downloads; Secure; HttpOnly; SameSite=Lax; Max-Age=600",
                )
                .setBody(
                    """{"expires_at_unix_ms": 1234, "apk": {"version_code": 9, "version_name": "0.3.1", "sha256": "${"ab".repeat(32)}"}}""",
                ),
        )
        val preview = client.preview("token-abc")
        assertEquals(9L, preview.versionCode)
        assertEquals("0.3.1", preview.versionName)
        assertEquals("ab".repeat(32), preview.sha256)
        assertEquals(1234L, preview.expiresAtUnixMs)
        assertEquals("ticket-value", preview.ticket)
        val recorded = server.takeRequest()
        assertEquals("/api/v1/device-enrollments/preview", recorded.path)
        assertTrue(recorded.body.readUtf8().contains("token-abc"))
        assertEquals("http://localhost:${server.port}", recorded.getHeader("Origin"))
    }

    @Test fun previewWithoutTicketCookieReportsNullTicket() = runTest {
        server.enqueue(
            MockResponse().setResponseCode(200).setBody(
                """{"expires_at_unix_ms": 1, "apk": {"version_code": 2, "version_name": "x", "sha256": "y"}}""",
            ),
        )
        assertNull(client.preview("token").ticket)
    }

    @Test fun previewFailuresThrowWithoutLeakingTheToken() = runTest {
        server.enqueue(MockResponse().setResponseCode(400).setBody("""{"error":"invalid"}"""))
        val error = runCatching { client.preview("secret-token") }.exceptionOrNull()
        assertTrue(error is IOException)
        assertTrue(error!!.message!!.contains("400"))
        assertTrue(!error.message!!.contains("secret-token"))
    }

    @Test fun downloadVerifiesDigestAndWritesTheFile() = runTest {
        val payload = ByteArray(200_000) { (it % 251).toByte() }
        val digest = MessageDigest.getInstance("SHA-256").digest(payload)
            .joinToString("") { "%02x".format(it) }
        server.enqueue(MockResponse().setResponseCode(200).setBody(Buffer().write(payload)))
        val destination = File(workDir, "updates/training-sensors.apk")
        val result = client.downloadVerified("ticket", digest, destination)
        assertEquals(destination, result)
        assertTrue(destination.readBytes().contentEquals(payload))
        val recorded = server.takeRequest()
        assertEquals("/downloads/training-sensors.apk", recorded.path)
        assertEquals("sensors_apk_ticket=ticket", recorded.getHeader("Cookie"))
    }

    @Test fun downloadRejectsDigestMismatchAndRemovesThePartialFile() = runTest {
        server.enqueue(MockResponse().setResponseCode(200).setBody("not-the-apk"))
        val destination = File(workDir, "updates/training-sensors.apk")
        val error = runCatching {
            client.downloadVerified("ticket", "cd".repeat(32), destination)
        }.exceptionOrNull()
        assertTrue(error is IOException)
        assertTrue(error!!.message!!.contains("integrity"))
        assertTrue(!destination.exists())
        assertTrue(workDir.walkTopDown().none { it.isFile })
    }

    @Test fun downloadSurfacesExpiredTicketStatus() = runTest {
        server.enqueue(MockResponse().setResponseCode(401).setBody("""{"error":"unauthorized"}"""))
        val error = runCatching {
            client.downloadVerified("stale", "ab".repeat(32), File(workDir, "u/apk"))
        }.exceptionOrNull()
        assertTrue(error is IOException)
        assertTrue(error!!.message!!.contains("401"))
    }

    @Test fun invalidExpectedDigestFailsClosedBeforeAnyRequest() = runTest {
        val error = runCatching {
            client.downloadVerified("ticket", "not-a-digest", File(workDir, "u/apk"))
        }.exceptionOrNull()
        assertTrue(error is IllegalArgumentException)
        assertEquals(0, server.requestCount)
    }
}
