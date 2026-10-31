package com.vineyardfinance.sensors.seca

import java.io.IOException

/** Independently implemented ct331 wire framing; no vendor implementation is included. */
object SecaProtocol {
    val rates = listOf(1000, 2000, 4000, 8000)

    fun retainAndValidate(frames: List<ByteArray>, retain: (ByteArray) -> Unit) {
        // A bad frame can share one socket read with subsequent valid frames. Preserve
        // the entire returned list before reporting failure and restarting the link.
        frames.forEach { frame ->
            var offset = 0
            while (offset < frame.size) {
                val end = minOf(frame.size, offset + 65535)
                retain(frame.copyOfRange(offset, end)); offset = end
            }
        }
        frames.firstOrNull { !valid(it) }?.let { throw IOException("seca frame/checksum failure; raw bytes saved; ${diagnostic(it)}") }
    }

    /** Structural numbers only: never include waveform bytes or configuration text in logs. */
    fun diagnostic(frame: ByteArray): String {
        val magic = frame.size >= 2 && frame[0] == 0x55.toByte() && frame[1] == 0xaa.toByte()
        val declared = if (frame.size >= 4) u16(frame, 2) else null
        val expected = if (frame.size >= 2) (-frame.dropLast(2).sumOf { it.toInt() and 255 }) and 65535 else null
        val actual = if (frame.size >= 2) u16(frame, frame.size - 2) else null
        val counter = if (frame.size >= 8) (4..7).fold(0L) { value, index -> (value shl 8) or (frame[index].toLong() and 255) } else null
        return "size=${frame.size} magic=$magic declared=$declared checksum_expected=$expected checksum_actual=$actual counter=$counter"
    }

    fun command(text: String, read: Boolean = false): ByteArray {
        require(text.all { it.code in 0..127 } && text.length <= 65521)
        val payload = text.toByteArray(Charsets.US_ASCII)
        val frame = ByteArray(payload.size + 14)
        frame[0] = 0x55; frame[1] = 0xaa.toByte()
        put16(frame, 2, frame.size)
        byteArrayOf(0x12, 0x34, 0x56, 0x78).copyInto(frame, 4)
        frame[9] = if (read) 0x52 else 0x57
        put16(frame, 10, payload.size)
        payload.copyInto(frame, 12)
        put16(frame, frame.size - 2, -frame.dropLast(2).sumOf { it.toInt() and 255 })
        return frame
    }

    fun valid(frame: ByteArray): Boolean = frame.size in 11..65535 &&
        frame[0] == 0x55.toByte() && frame[1] == 0xaa.toByte() &&
        u16(frame, 2) == frame.size &&
        ((frame.dropLast(2).sumOf { it.toInt() and 255 } + u16(frame, frame.size - 2)) and 65535) == 0

    /** Configuration replies are inspected structurally, never by searching arbitrary ECG bytes. */
    fun configuration(frame: ByteArray): Map<String, String> {
        if (!valid(frame)) return emptyMap()
        val result = mutableMapOf<String, String>()
        var offset = 9
        while (offset < frame.size - 2) {
            when (frame[offset++].toInt() and 255) {
                0x41 -> {
                    if (offset + 2 > frame.size - 2) return result
                    val size = u16(frame, offset); offset += 2
                    if (offset + size > frame.size - 2) return result
                    String(frame, offset, size, Charsets.US_ASCII).split('\n', '\r', ';', '\u0000').forEach { line ->
                        val pair = line.trim().split('=', limit = 2)
                        if (pair.size == 2) result[pair[0].trim().lowercase()] = pair[1].trim()
                    }
                    offset += size
                }
                0x4f, 0x4d, 0x4b -> offset += 1
                0x50 -> offset += 3
                0x53, 0x73 -> offset += 5
                in 0xc1..0xc6, 0xca, 0xcb -> {
                    if (offset >= frame.size - 2) return result
                    val width = frame[offset].toInt() and 255
                    val deltas = when (width) { 0 -> 0; 4 -> 16; 8 -> 31; 12 -> 48; 16 -> 62; 20 -> 78; 24 -> 93; else -> return result }
                    offset += 4 + deltas
                }
                else -> return result // Unknown metadata does not invalidate retained raw bytes.
            }
        }
        return result
    }

    internal fun u16(bytes: ByteArray, offset: Int) = ((bytes[offset].toInt() and 255) shl 8) or (bytes[offset + 1].toInt() and 255)
    internal fun put16(bytes: ByteArray, offset: Int, value: Int) {
        bytes[offset] = (value ushr 8).toByte(); bytes[offset + 1] = value.toByte()
    }
}

/** A framing failure returns all buffered bytes to the journal before reconnecting. */
class SecaFramer {
    private var pending = ByteArray(0)
    fun feed(bytes: ByteArray, count: Int = bytes.size): List<ByteArray> {
        require(count in 0..bytes.size)
        pending += bytes.copyOf(count)
        val frames = mutableListOf<ByteArray>()
        while (pending.size >= 4) {
            val length = SecaProtocol.u16(pending, 2)
            if (pending[0] != 0x55.toByte() || pending[1] != 0xaa.toByte() || length < 11) {
                frames += pending
                pending = ByteArray(0)
                break
            }
            if (pending.size < length) break
            frames += pending.copyOf(length)
            pending = pending.copyOfRange(length, pending.size)
        }
        return frames
    }
    fun remaining(): ByteArray = pending.copyOf()
}
