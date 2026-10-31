package com.vineyardfinance.sensors.seca

import org.junit.Assert.*
import org.junit.Test

class SecaLivenessTest {
    private fun group(counter: Int = 1, width: Int = 0, size: Int = 0): ByteArray {
        val body = listOf(0xc6, 0xca, 0xcb, 0xc2, 0xc3, 0xc4, 0xc5, 0xc1).fold(ByteArray(0)) { all, tag -> all + byteArrayOf(tag.toByte(), width.toByte(), 0, 0, 0) + ByteArray(size) }
        val frame = SecaProtocolTest.rx(body)
        frame[7] = counter.toByte()
        SecaProtocol.put16(frame, frame.size - 2, -frame.dropLast(2).sumOf { it.toInt() and 255 })
        return frame
    }
    @Test fun statusTrafficAndRepeatedCountersCannotHideEcgStalls() {
        val live = SecaLiveness(0)
        assertNull(live.failure(10_000)); assertNotNull(live.failure(10_001))
        live.bytes(10_001); live.frame(group(), 10_001)
        assertNull(live.failure(10_002))
        live.started(20_000)
        live.frame(group(), 20_001)
        live.bytes(30_002)
        live.frame(SecaProtocolTest.rx(byteArrayOf(0x4d, 0)), 30_002)
        live.frame(group(), 30_002)
        assertTrue(live.failure(30_002)!!.contains("ECG samples stopped"))
        live.frame(group(2), 30_003)
        assertNull(live.failure(30_004))
    }
    @Test fun counterRequiresFullStructurallyBoundedGroup() {
        listOf(0 to 0, 4 to 16, 8 to 31, 12 to 48, 16 to 62, 20 to 78, 24 to 93).forEach { (width, count) ->
            assertEquals(7L, SecaLiveness.ecgCounter(group(7, width, count)))
        }
        listOf(byteArrayOf(), byteArrayOf(0x41), byteArrayOf(0x41, 0, 99), byteArrayOf(0xc1.toByte()), byteArrayOf(0xc1.toByte(), 2, 0, 0, 0), byteArrayOf(0xc6.toByte(), 0, 0, 0, 0), byteArrayOf(0x50), byteArrayOf(0x22)).forEach {
            assertNull(SecaLiveness.ecgCounter(SecaProtocolTest.rx(it)))
        }
        assertNull(SecaLiveness.ecgCounter(byteArrayOf(1)))
        val status = byteArrayOf(0x41, 0, 1, 65, 0x4f, 0, 0x4d, 0, 0x4b, 0, 0x50, 0, 0, 50, 0x53, 0, 0, 0, 0, 0, 0x73, 0, 0, 0, 0, 0)
        assertNull(SecaLiveness.ecgCounter(SecaProtocolTest.rx(status)))
    }
}
