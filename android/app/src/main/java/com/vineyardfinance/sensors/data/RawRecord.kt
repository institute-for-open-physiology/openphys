package com.vineyardfinance.sensors.data

import android.os.SystemClock
import com.google.gson.JsonObject
import java.util.UUID

data class CaptureContext(
    val captureSessionId: String,
    val writerEpochId: String,
    val epochStartElapsedNs: Long = SystemClock.elapsedRealtimeNanos(),
    val connectionId: String? = null,
    val physicalDeviceId: String? = null,
    val blePeripheralId: String? = null,
)

data class RawRecord(
    val recordId: String = UUID.randomUUID().toString(),
    val connectionId: String?,
    val collectorUnixNs: Long,
    val collectorMonotonicNs: Long,
    val physicalDeviceId: String?,
    val blePeripheralId: String?,
    val kind: String,
    val body: JsonObject,
) {
    companion object {
        fun now(context: CaptureContext, kind: String, body: JsonObject): RawRecord {
            val unixNs = System.currentTimeMillis() * 1_000_000L
            return RawRecord(
                connectionId = context.connectionId,
                collectorUnixNs = unixNs,
                collectorMonotonicNs = (SystemClock.elapsedRealtimeNanos() - context.epochStartElapsedNs).coerceAtLeast(0),
                physicalDeviceId = context.physicalDeviceId,
                blePeripheralId = context.blePeripheralId,
                kind = kind,
                body = body,
            )
        }
    }
}
