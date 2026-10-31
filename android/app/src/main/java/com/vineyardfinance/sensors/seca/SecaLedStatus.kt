package com.vineyardfinance.sensors.seca

import java.io.IOException

/** Experimental vendor-pattern traffic. O=0 is not proof of contact, especially with openlead=0. */
class SecaLedStatus {
    private var latest: Pair<Int, Long>? = null

    @Synchronized fun observe(frame: ByteArray, receiveMs: Long) {
        rawStatus(frame)?.let { latest = it to receiveMs }
    }

    @Synchronized fun command(nowMs: () -> Long): SecaHostCommand {
        val (raw, received) = latest ?: throw IOException("LED traffic experiment has no validated O status")
        // Sampling before this monitor can race a newer reader observation.
        return SecaHostCommand(payload(raw), false, received).also { it.validateBeforeWrite(nowMs()) }
    }

    companion object {
        /** Raw wire O, not the rotated channel/contact mask used in ECG decoding. */
        fun payload(raw: Int): String {
            require(raw in 0..255)
            val bank = if (raw and 3 == 3) "111111111" else buildString {
                append('0') // Vendor logical slot1 stays zero unless the shared fault applies.
                append(if (raw and 3 == 1) '1' else '0')
                append(if (raw and 3 == 2) '1' else '0')
                for (bit in 2..7) append(if (raw and (1 shl bit) != 0) '1' else '0')
            }
            return "ledstatus=" + bank + bank.map { if (it == '1') '0' else '1' }.joinToString("") + "0".repeat(16)
        }

        /** Whole-frame typed walk: never search configuration text or compressed samples for O. */
        fun rawStatus(frame: ByteArray): Int? {
            if (!SecaProtocol.valid(frame)) return null
            var offset = 9
            val end = frame.size - 2
            var result: Int? = null
            var channels = 0
            while (offset < end) {
                val field = frame[offset++].toInt() and 255
                val size = when (field) {
                    0x41 -> {
                        if (offset + 2 > end) return null
                        val length = SecaProtocol.u16(frame, offset); offset += 2; length
                    }
                    0x4f -> { if (offset >= end) return null; result = frame[offset].toInt() and 255; 1 }
                    0x4d, 0x4b -> 1
                    0x50 -> 3
                    0x53, 0x73 -> 5
                    in 0xc1..0xc6, 0xca, 0xcb -> {
                        if (offset >= end || (field == 0xc6 && channels != 0)) return null
                        val bit = 1 shl when (field) { 0xca -> 6; 0xcb -> 7; else -> field - 0xc1 }
                        if (channels and bit != 0) return null
                        channels = channels or bit
                        if (field == 0xc1) { if (channels != 255) return null; channels = 0 }
                        val width = frame[offset].toInt() and 255
                        val packed = when (width) { 0 -> 0; 4 -> 16; 8 -> 31; 12 -> 48; 16 -> 62; 20 -> 78; 24 -> 93; else -> return null }
                        4 + packed
                    }
                    else -> return null
                }
                if (size > end - offset) return null
                offset += size
            }
            return if (channels == 0) result else null
        }
    }
}

data class SecaHostCommand(val payload: String, val read: Boolean, val observationMs: Long? = null) {
    /** Recheck this exact archived observation after journal ACK, never substitute a newer mask. */
    fun validateBeforeWrite(nowMs: Long) {
        observationMs?.let {
            if (nowMs - it !in 0..3000L) throw IOException("LED traffic experiment O status stale; recording stopped")
        }
    }
}

fun secaKeepaliveCommand(experimentalLed: Boolean, status: SecaLedStatus, nowMs: () -> Long): SecaHostCommand =
    if (experimentalLed) status.command(nowMs) else SecaHostCommand("", true)
