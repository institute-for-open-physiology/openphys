package com.vineyardfinance.sensors.data

import android.app.Application
import com.google.gson.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class RecordJsonTest {
    @Test fun emitsFrozenRecordOrderAndPaddedBase64() {
        val record = RawRecord(
            recordId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            connectionId = null,
            collectorUnixNs = 10,
            collectorMonotonicNs = 20,
            physicalDeviceId = "h10_x",
            blePeripheralId = "aa:bb",
            kind = "collector_event",
            body = JsonObject().apply { addProperty("event", "test") },
        )
        val json = RecordJson.encode(7, record)
        assertTrue(json.startsWith("{\"record_id\":\"aaaaaaaa"))
        assertTrue(json, json.indexOf("source_seq") < json.indexOf("connection_id"))
        assertTrue(json.endsWith("\"body\":{\"event\":\"test\"}}"))
        assertEquals("AQI=", RecordJson.payload(byteArrayOf(1, 2)))
    }

    @Test fun diagnosticFieldsAreLexicographicallyOrdered() {
        assertEquals("{\"a\":true,\"m\":null,\"z\":3}", RecordJson.fields(mapOf("z" to 3, "a" to true, "m" to null)).toString())
    }
}
