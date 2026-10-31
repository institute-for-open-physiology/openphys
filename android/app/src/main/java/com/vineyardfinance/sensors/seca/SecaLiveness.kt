package com.vineyardfinance.sensors.seca

/** Status traffic cannot masquerade as continuing ECG acquisition. Monotonic milliseconds only. */
class SecaLiveness(startMs: Long) {
    private var lastBytes = startMs
    private var lastEcg: Long? = null
    private var lastCounter: Long? = null
    @Synchronized fun bytes(nowMs: Long) { lastBytes = nowMs }
    @Synchronized fun started(nowMs: Long) { lastEcg = nowMs; lastCounter = null }
    @Synchronized fun frame(frame: ByteArray, nowMs: Long) {
        if (lastEcg == null) return
        val counter = ecgCounter(frame) ?: return
        if (counter != lastCounter) { lastEcg = nowMs; lastCounter = counter }
    }
    @Synchronized fun failure(nowMs: Long): String? = when {
        nowMs - lastBytes > 10_000 -> "No Bluetooth bytes for 10 seconds"
        lastEcg?.let { nowMs - it > 10_000 } == true -> "ECG samples stopped for 10 seconds despite Bluetooth traffic"
        else -> null
    }

    companion object {
        /** Locate a complete eight-channel group using typed field lengths. */
        fun ecgCounter(frame: ByteArray): Long? {
            if (!SecaProtocol.valid(frame)) return null
            var offset = 9
            var channels = 0
            var complete = false
            val end = frame.size - 2
            while (offset < end) {
                val tag = frame[offset++].toInt() and 255
                when (tag) {
                    0x41 -> {
                        if (offset + 2 > end) return null
                        val length = SecaProtocol.u16(frame, offset); offset += 2 + length
                    }
                    0x4f, 0x4d, 0x4b -> offset++
                    0x50 -> offset += 3
                    0x53, 0x73 -> offset += 5
                    in 0xc1..0xc6, 0xca, 0xcb -> {
                        if (offset + 4 > end) return null
                        val width = frame[offset].toInt() and 255
                        val delta = when (width) { 0 -> 0; 4 -> 16; 8 -> 31; 12 -> 48; 16 -> 62; 20 -> 78; 24 -> 93; else -> return null }
                        offset += 4 + delta
                        if (tag == 0xc6) channels = 0
                        val index = when (tag) { 0xca -> 6; 0xcb -> 7; else -> tag - 0xc1 }
                        channels = channels or (1 shl index)
                        if (tag == 0xc1 && channels == 255 && offset <= end) complete = true
                    }
                    else -> return null
                }
                if (offset > end) return null
            }
            return if (complete) (4..7).fold(0L) { value, index -> (value shl 8) or (frame[index].toLong() and 255) } else null
        }
    }
}
