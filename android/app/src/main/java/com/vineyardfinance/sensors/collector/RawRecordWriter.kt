package com.vineyardfinance.sensors.collector

import android.os.SystemClock
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.data.QueueRepository
import com.vineyardfinance.sensors.data.RawRecord

class RawRecordWriter(
    private val queue: QueueRepository,
    val baseContext: CaptureContext,
    val signals: LiveSignals? = null,
) {
    @Volatile var connectionId: String? = null
    @Volatile var peripheralId: String? = null

    fun context(): CaptureContext = baseContext.copy(connectionId = connectionId, blePeripheralId = peripheralId)

    suspend fun append(kind: String, body: JsonObject): Long = queue.append(context(), RawRecord.now(context(), kind, body))

    suspend fun appendCaptured(
        kind: String,
        body: JsonObject,
        unixNs: Long,
        elapsedRealtimeNs: Long,
    ): Long = queue.append(
        context(),
        RawRecord(
            connectionId = connectionId,
            collectorUnixNs = unixNs,
            collectorMonotonicNs = (elapsedRealtimeNs - baseContext.epochStartElapsedNs).coerceAtLeast(0),
            physicalDeviceId = baseContext.physicalDeviceId,
            blePeripheralId = peripheralId,
            kind = kind,
            body = body,
        ),
    )

    suspend fun event(event: String, severity: String, detail: String, fields: JsonObject = JsonObject()) = append(
        "collector_event",
        JsonObject().apply {
            addProperty("event", event)
            addProperty("severity", severity)
            addProperty("detail", detail)
            add("fields", fields)
        },
    )

    companion object {
        fun capturedTimes(): Pair<Long, Long> = System.currentTimeMillis() * 1_000_000L to SystemClock.elapsedRealtimeNanos()
    }
}
