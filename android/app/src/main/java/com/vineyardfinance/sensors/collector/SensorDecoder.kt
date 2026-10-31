package com.vineyardfinance.sensors.collector

/** Display decoding leaves the original GATT bytes in the durable queue. */
object SensorDecoder {
    data class HeartRate(
        val bpm: Int,
        val contactSupported: Boolean,
        val contactDetected: Boolean,
        val rrMs: List<Double>,
    ) {
        val hasHeartRate: Boolean
            get() = bpm > 0 && (!contactSupported || contactDetected)

        fun contact(age: Double): Boolean? =
            when {
                age < 0 || age >= 2.5 || !contactSupported -> null
                !contactDetected -> false
                hasHeartRate -> true
                else -> null
            }
    }

    data class Frame(
        val type: Int,
        val timestampNs: Long,
        val format: Int,
        val compressed: Boolean,
        val content: ByteArray,
    )

    fun parseHeartRate(data: ByteArray): HeartRate? {
        if (data.size < 2) return null
        val flags = data[0].toInt() and 255
        var index = if (flags and 1 != 0) 3 else 2
        if (data.size < index) return null
        val bpm = unsigned(data, 1, index - 1).toInt()
        if (flags and 8 != 0) index += 2
        val rr = mutableListOf<Double>()
        if (flags and 16 != 0)
            while (index + 1 < data.size) {
                rr += unsigned(data, index, 2) * 1000.0 / 1024
                index += 2
            }
        return HeartRate(bpm, flags and 4 != 0, flags and 2 != 0, rr)
    }

    fun parseFrame(data: ByteArray): Frame? {
        if (data.size < 10) return null
        val stamp = unsigned(data, 1, 8)
        if (stamp < 0 || stamp > 150L * 366 * 86400 * 1_000_000_000) return null
        return Frame(
            data[0].toInt() and 63,
            stamp,
            data[9].toInt() and 127,
            data[9].toInt() and 128 != 0,
            data.copyOfRange(10, data.size),
        )
    }

    fun decodeECG(frame: Frame): List<Int>? {
        if (frame.type != 0 || frame.compressed || frame.format != 0) return null
        return (0 until frame.content.size - 2 step 3).map { signed(frame.content, it, 3) }
    }

    fun decodeACC(frame: Frame): List<List<Int>>? {
        if (frame.type != 2) return null
        if (!frame.compressed) {
            if (frame.format > 2) return null
            val width = frame.format + 1
            return (0 until frame.content.size - width * 3 + 1 step width * 3).map { offset ->
                (0..2).map { signed(frame.content, offset + it * width, width) }
            }
        }
        if (frame.format > 1 || frame.content.size < 6) return null
        val data = frame.content
        var current = (0..2).map { signed(data, it * 2, 2) }
        val samples = mutableListOf(current)
        var offset = 6
        while (offset + 2 <= data.size) {
            val bits = data[offset++].toInt() and 255
            val count = data[offset++].toInt() and 255
            if (bits == 0 || count == 0) continue
            if (bits > 32) return null
            val length = (bits * count * 3 + 7) / 8
            if (offset + length > data.size) return null
            repeat(count) { sample ->
                current =
                    (0..2).map { channel ->
                        var value = 0
                        repeat(bits) { i ->
                            val bit = (sample * 3 + channel) * bits + i
                            value =
                                value or
                                    (((data[offset + bit / 8].toInt() ushr (bit % 8)) and 1) shl i)
                        }
                        current[channel] +
                            if (bits < 32) (value shl (32 - bits)) shr (32 - bits) else value
                    }
                samples += current
            }
            offset += length
        }
        return if (frame.format == 0) samples.map { row -> row.map { it * 1000 } } else samples
    }

    private fun unsigned(data: ByteArray, start: Int, width: Int): Long =
        (0 until width).fold(0L) { value, i ->
            value or ((data[start + i].toLong() and 255) shl (i * 8))
        }

    private fun signed(data: ByteArray, start: Int, width: Int): Int =
        (unsigned(data, start, width).toInt() shl (32 - width * 8)) shr (32 - width * 8)
}
