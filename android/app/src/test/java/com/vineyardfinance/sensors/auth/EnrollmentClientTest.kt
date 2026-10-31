package com.vineyardfinance.sensors.auth

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.google.gson.JsonParser
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.network.HttpJson
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
class EnrollmentClientTest {
    private lateinit var server: MockWebServer
    private lateinit var context: Context
    private lateinit var identity: AppIdentity
    private lateinit var store: FakeCredentialStore
    private lateinit var client: EnrollmentClient
    private val token = "t".repeat(43)
    private val credential = "c".repeat(43)
    private val exchangeId = "11111111-1111-4111-8111-111111111111"
    private val credentialId = "22222222-2222-4222-8222-222222222222"

    @Before fun setUp() {
        server = MockWebServer().apply { start() }
        context = ApplicationProvider.getApplicationContext()
        context.getSharedPreferences("installation_identity", Context.MODE_PRIVATE).edit().clear().commit()
        identity = AppIdentity(context)
        store = FakeCredentialStore()
        client = EnrollmentClient(
            HttpJson(server.url("/").toString().trimEnd('/')),
            identity,
            store,
        ) {
            EnrollmentMetadata(
                displayLabel = "Galaxy test",
                androidManufacturer = "Samsung",
                androidModel = "SM-A546B",
                androidSdk = 34,
                appPackage = "com.vineyardfinance.sensors",
                appVersionName = "0.2.0",
                appVersionCode = 2,
            )
        }
    }

    @After fun tearDown() { server.shutdown() }

    @Test fun postsExactSharedExchangeAndStoresOnlyValidatedCredential() = runTest {
        server.enqueue(success())

        val result = client.exchange(token, exchangeId)

        assertEquals(identity.installationId, result.installationId)
        assertEquals(credentialId, result.credentialId)
        assertEquals(credential, store.currentCredential())
        val request = server.takeRequest()
        assertEquals(EnrollmentClient.EXCHANGE_PATH, request.path)
        assertNull(request.getHeader("Authorization"))
        assertFalse(request.path!!.contains(token))
        val body = JsonParser.parseString(request.body.readUtf8()).asJsonObject
        assertEquals(
            setOf(
                "enrollment_token",
                "installation_id",
                "exchange_id",
                "display_label",
                "android_manufacturer",
                "android_model",
                "android_sdk",
                "app_package",
                "app_version_name",
                "app_version_code",
            ),
            body.keySet(),
        )
        assertEquals(token, body["enrollment_token"].asString)
        assertEquals(identity.installationId, body["installation_id"].asString)
        assertEquals(exchangeId, body["exchange_id"].asString)
        assertEquals("Galaxy test", body["display_label"].asString)
        assertEquals("Samsung", body["android_manufacturer"].asString)
        assertEquals("SM-A546B", body["android_model"].asString)
        assertEquals(34, body["android_sdk"].asInt)
        assertEquals("com.vineyardfinance.sensors", body["app_package"].asString)
        assertEquals("0.2.0", body["app_version_name"].asString)
        assertEquals(2L, body["app_version_code"].asLong)
    }

    @Test fun mismatchMalformedSuccessAndBadInputsNeverReplaceCredential() = runTest {
        store.storeCredential("z".repeat(43))
        server.enqueue(success(installationId = UUID.randomUUID().toString()))
        assertTrue(runCatching { client.exchange(token, exchangeId) }.isFailure)
        assertEquals("z".repeat(43), store.currentCredential())

        server.enqueue(success(deviceCredential = "too-short"))
        assertTrue(runCatching { client.exchange(token, exchangeId) }.isFailure)
        assertEquals("z".repeat(43), store.currentCredential())

        assertTrue(runCatching { client.exchange("bad", exchangeId) }.isFailure)
        assertTrue(runCatching { client.exchange(token, "bad-uuid") }.isFailure)
        assertEquals(2, server.requestCount)
    }

    @Test fun rejectsNegativeVersionAndAllowsNullDisplayLabelBeforePosting() = runTest {
        val invalid = EnrollmentClient(HttpJson(server.url("/").toString().trimEnd('/')), identity, store) {
            EnrollmentMetadata(null, "m", "x", 34, "p", "v", -1)
        }
        assertTrue(runCatching { invalid.exchange(token, exchangeId) }.isFailure)
        assertEquals(0, server.requestCount)

        val nullable = EnrollmentClient(HttpJson(server.url("/").toString().trimEnd('/')), identity, store) {
            EnrollmentMetadata(null, "m", "x", 34, "p", "v", 2)
        }
        server.enqueue(success())
        nullable.exchange(token, exchangeId)
        val body = JsonParser.parseString(server.takeRequest().body.readUtf8()).asJsonObject
        assertTrue(!body.has("display_label") || body["display_label"].isJsonNull)
    }

    @Test fun retainsEveryMetadataFieldAtItsExactAsciiByteBoundary() = runTest {
        val metadata = EnrollmentMetadata(
            displayLabel = "d".repeat(80),
            androidManufacturer = "m".repeat(80),
            androidModel = "x".repeat(80),
            androidSdk = 34,
            appPackage = "p".repeat(200),
            appVersionName = "v".repeat(80),
            appVersionCode = 2,
        )
        server.enqueue(success())

        clientFor(metadata).exchange(token, exchangeId)

        val body = JsonParser.parseString(server.takeRequest().body.readUtf8()).asJsonObject
        assertUtf8(body["display_label"].asString, 80, "d".repeat(80))
        assertUtf8(body["android_manufacturer"].asString, 80, "m".repeat(80))
        assertUtf8(body["android_model"].asString, 80, "x".repeat(80))
        assertUtf8(body["app_package"].asString, 200, "p".repeat(200))
        assertUtf8(body["app_version_name"].asString, 80, "v".repeat(80))
    }

    @Test fun truncatesEveryMetadataFieldAtItsFieldSpecificAsciiByteBoundary() = runTest {
        val metadata = EnrollmentMetadata(
            displayLabel = "d".repeat(81),
            androidManufacturer = "m".repeat(81),
            androidModel = "x".repeat(81),
            androidSdk = 34,
            appPackage = "p".repeat(201),
            appVersionName = "v".repeat(81),
            appVersionCode = 2,
        )
        server.enqueue(success())

        clientFor(metadata).exchange(token, exchangeId)

        val body = JsonParser.parseString(server.takeRequest().body.readUtf8()).asJsonObject
        assertUtf8(body["display_label"].asString, 80, "d".repeat(80))
        assertUtf8(body["android_manufacturer"].asString, 80, "m".repeat(80))
        assertUtf8(body["android_model"].asString, 80, "x".repeat(80))
        assertUtf8(body["app_package"].asString, 200, "p".repeat(200))
        assertUtf8(body["app_version_name"].asString, 80, "v".repeat(80))
    }

    @Test fun truncatesMultibyteMetadataWithoutSplittingCodePointsOrSurrogatePairs() = runTest {
        val metadata = EnrollmentMetadata(
            displayLabel = "é".repeat(41),
            androidManufacturer = "a".repeat(79) + "é" + "tail",
            androidModel = "😀".repeat(21),
            androidSdk = 34,
            appPackage = "p".repeat(198) + "😀" + "tail",
            appVersionName = "界".repeat(27),
            appVersionCode = 2,
        )
        server.enqueue(success())

        clientFor(metadata).exchange(token, exchangeId)

        val body = JsonParser.parseString(server.takeRequest().body.readUtf8()).asJsonObject
        assertUtf8(body["display_label"].asString, 80, "é".repeat(40))
        assertUtf8(body["android_manufacturer"].asString, 79, "a".repeat(79))
        assertUtf8(body["android_model"].asString, 80, "😀".repeat(20))
        assertUtf8(body["app_package"].asString, 198, "p".repeat(198))
        assertUtf8(body["app_version_name"].asString, 78, "界".repeat(26))
        assertFalse(Character.isHighSurrogate(body["android_model"].asString.last()))
    }

    @Test fun preservesBackendMachineCodeWithoutExposingResponseBody() = runTest {
        server.enqueue(
            MockResponse().setResponseCode(400)
                .setBody("{\"code\":\"${EnrollmentManager.ENROLLMENT_LINK_TERMINAL_CODE}\",\"error\":\"redacted\"}"),
        )

        val failure = runCatching { client.exchange(token, exchangeId) }.exceptionOrNull()

        assertTrue(failure is HttpJson.HttpStatusException)
        failure as HttpJson.HttpStatusException
        assertEquals(400, failure.status)
        assertEquals(EnrollmentManager.ENROLLMENT_LINK_TERMINAL_CODE, failure.machineCode)
        assertFalse(failure.message.orEmpty().contains("redacted"))
    }

    private fun success(
        installationId: String = identity.installationId,
        deviceCredential: String = credential,
    ) = MockResponse().setResponseCode(200).setBody(
        """{"installation_id":"$installationId","credential_id":"$credentialId","device_credential":"$deviceCredential"}""",
    )

    private fun clientFor(metadata: EnrollmentMetadata) = EnrollmentClient(
        HttpJson(server.url("/").toString().trimEnd('/')),
        identity,
        store,
    ) { metadata }

    private fun assertUtf8(actual: String, expectedBytes: Int, expected: String) {
        assertEquals(expected, actual)
        assertEquals(expectedBytes, actual.toByteArray(Charsets.UTF_8).size)
    }

    private class FakeCredentialStore(initial: String? = null) : DeviceCredentialStore {
        private var value = initial
        private val mutable = MutableStateFlow(initial != null)
        override val connected: StateFlow<Boolean> = mutable
        override fun currentCredential(): String? = value
        override fun storeCredential(credential: String) { value = credential; mutable.value = true }
        override fun clearIfCurrent(rejectedCredential: String): Boolean {
            if (value != rejectedCredential) return false
            clear(); return true
        }
        override fun clear() { value = null; mutable.value = false }
    }
}
