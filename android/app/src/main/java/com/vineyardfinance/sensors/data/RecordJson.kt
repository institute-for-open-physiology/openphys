package com.vineyardfinance.sensors.data

import android.util.Base64
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import java.util.TreeMap

object RecordJson {
    private val gson: Gson = GsonBuilder().serializeNulls().disableHtmlEscaping().create()

    fun encode(sourceSeq: Long, record: RawRecord): String = JsonObject().apply {
        addProperty("record_id", record.recordId)
        addProperty("source_seq", sourceSeq)
        nullable("connection_id", record.connectionId)
        addProperty("collector_unix_ns", record.collectorUnixNs)
        addProperty("collector_monotonic_ns", record.collectorMonotonicNs)
        nullable("physical_device_id", record.physicalDeviceId)
        nullable("ble_peripheral_id", record.blePeripheralId)
        addProperty("kind", record.kind)
        add("body", record.body)
    }.let(gson::toJson)

    fun payload(bytes: ByteArray): String = Base64.encodeToString(bytes, Base64.NO_WRAP)

    fun body(build: JsonObject.() -> Unit): JsonObject = JsonObject().apply(build)

    fun fields(values: Map<String, Any?>): JsonObject = JsonObject().apply {
        TreeMap(values).forEach { (key, value) ->
            when (value) {
                null -> add(key, JsonNull.INSTANCE)
                is Boolean -> addProperty(key, value)
                is Number -> addProperty(key, value)
                else -> addProperty(key, value.toString())
            }
        }
    }

    fun JsonObject.nullable(name: String, value: String?) {
        if (value == null) add(name, JsonNull.INSTANCE) else addProperty(name, value)
    }
}
