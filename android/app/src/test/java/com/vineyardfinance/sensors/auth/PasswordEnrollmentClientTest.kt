package com.vineyardfinance.sensors.auth

import com.google.gson.JsonParser
import com.vineyardfinance.sensors.network.HttpJson
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit

class PasswordEnrollmentClientTest {
    private lateinit var server: MockWebServer
    private lateinit var client: PasswordEnrollmentClient
    private val token = "t".repeat(43)
    @Before fun setup() {
        server = MockWebServer().apply { start() }
        client = PasswordEnrollmentClient(server.url("/").toString())
    }
    @After fun teardown() { server.shutdown() }
    private fun login() = MockResponse().setBody("{\"ok\":true}")
        .addHeader("Set-Cookie", "sensors_access=access; Path=/; HttpOnly")
        .addHeader("Set-Cookie", "sensors_csrf=csrf; Path=/")
        .addHeader("Set-Cookie", "sensors_refresh=refresh; Path=/api/v1/auth")
        .addHeader("Set-Cookie", "expired=old; Path=/; Max-Age=0")
    private fun enrollment(link: String = "https://sensors-poc.vineyard-finance.com/enroll#$token") =
        MockResponse().setBody("{\"enrollment_url\":\"$link\"}")

    @Test fun nativeLoginUsesCookiesAndOriginCsrfWithoutLeakingThemIntoSubsequentLogin() = runTest {
        repeat(2) {
            server.enqueue(login())
            server.enqueue(enrollment())
            assertEquals(token, client.createToken(" user@example.com ", " password "))
            val login = server.takeRequest(2, TimeUnit.SECONDS)!!
            assertEquals("/api/v1/auth/password/login", login.path)
            assertEquals(server.url("/").toString().trimEnd('/'), login.getHeader("Origin"))
            assertNull(login.getHeader("Cookie"))
            assertNull(login.getHeader("X-CSRF-Token"))
            assertNull(login.getHeader("Authorization"))
            val body = JsonParser.parseString(login.body.readUtf8()).asJsonObject
            assertEquals("user@example.com", body["email"].asString)
            assertEquals(" password ", body["password"].asString)
            val enroll = server.takeRequest(2, TimeUnit.SECONDS)!!
            assertEquals("/api/v1/device-enrollments", enroll.path)
            assertEquals("csrf", enroll.getHeader("X-CSRF-Token"))
            assertEquals("sensors_access=access; sensors_csrf=csrf", enroll.getHeader("Cookie"))
            assertEquals("{}", enroll.body.readUtf8())
            assertFalse(enroll.path!!.contains(token))
        }
    }
    @Test fun refusesRedirectAndDoesNotPostPasswordToOtherHost() = runTest {
        val other = MockWebServer().apply { start() }
        try {
            server.enqueue(MockResponse().setResponseCode(307).addHeader("Location", other.url("/steal")))
            val failure = runCatching { client.createToken("a@b.test", "secret") }.exceptionOrNull()
            assertEquals(307, (failure as HttpJson.HttpStatusException).status)
            assertEquals(0, other.requestCount)
        } finally { other.shutdown() }
    }
    @Test fun loginFailureDoesNotCreateEnrollmentAndCanStartCleanlyAgain() = runTest {
        server.enqueue(login().setResponseCode(401).setBody("{\"code\":\"invalid_credentials\"}"))
        val failure = runCatching { client.createToken("a@b.test", "secret") }.exceptionOrNull()
        assertEquals("invalid_credentials", (failure as HttpJson.HttpStatusException).machineCode)
        assertEquals(1, server.requestCount)
        server.takeRequest()
        server.enqueue(login())
        server.enqueue(enrollment())
        assertEquals(token, client.createToken("a@b.test", "correct"))
        assertNull(server.takeRequest().getHeader("Cookie"))
    }
    @Test fun rejectsEnrollmentFailureAndInvalidOrForeignLinks() = runTest {
        val responses = listOf(
            MockResponse().setResponseCode(429), enrollment("https://evil.test/enroll#$token"),
            enrollment("https://sensors-poc.vineyard-finance.com/enroll#bad"), MockResponse().setBody("{}"),
        )
        for (response in responses) {
            server.enqueue(login())
            server.enqueue(response)
            assertTrue(runCatching { client.createToken("a@b.test", "secret") }.isFailure)
        }
    }
}
