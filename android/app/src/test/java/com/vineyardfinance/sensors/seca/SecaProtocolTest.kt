package com.vineyardfinance.sensors.seca

import org.junit.Assert.*
import org.junit.Test

class SecaProtocolTest {
    @Test fun commandMatchesIndependentKnownWireVector() {
        val read = SecaProtocol.command("", read = true)
        assertEquals("55aa000e1234567800520000fd8d", read.joinToString("") { "%02x".format(it.toInt() and 255) })
        val write = SecaProtocol.command("mode=run")
        assertTrue(SecaProtocol.valid(write))
        assertEquals(0x57, write[9].toInt())
        assertEquals("mode=run", String(write, 12, 8))
        assertThrows(IllegalArgumentException::class.java) { SecaProtocol.command("é") }
        assertThrows(IllegalArgumentException::class.java) { SecaProtocol.command("x".repeat(65522)) }
    }
    @Test fun validatesHeaderLengthAndChecksumWithoutAcceptingTruncatedFrames() {
        val good = rx(byteArrayOf(0x4d, 0))
        assertTrue(SecaProtocol.valid(good))
        listOf(0, 1, 2, good.lastIndex).forEach { at ->
            val bad = good.copyOf(); bad[at] = (bad[at].toInt() xor 1).toByte()
            assertFalse(SecaProtocol.valid(bad))
        }
        assertFalse(SecaProtocol.valid(ByteArray(10)))
        assertFalse(SecaProtocol.valid(ByteArray(65536)))
        assertTrue(SecaProtocol.configuration(ByteArray(10)).isEmpty())
    }
    @Test fun streamAssemblerPreservesEveryByteAcrossSplitsAndCoalescing() {
        val one = rx(byteArrayOf(0x4d, 0)); val two = rx(byteArrayOf(0x50, 0x10, 0, 85))
        val wire = one + two
        for (split in wire.indices) {
            val framer = SecaFramer()
            val frames = framer.feed(wire.copyOf(split)) + framer.feed(wire.copyOfRange(split, wire.size))
            assertEquals(2, frames.size); assertArrayEquals(one, frames[0]); assertArrayEquals(two, frames[1])
            assertTrue(framer.remaining().isEmpty())
        }
        val framer = SecaFramer()
        val malformed = byteArrayOf(1, 2, 3, 4, 5)
        val frames = framer.feed(one + malformed)
        assertArrayEquals(one, frames[0]); assertArrayEquals(malformed, frames[1])
        assertFalse(SecaProtocol.valid(frames[1]))
        assertArrayEquals(byteArrayOf(0x55, 0xaa.toByte(), 0, 0), SecaFramer().feed(byteArrayOf(0x55, 0xaa.toByte(), 0, 0)).single())
        assertThrows(IllegalArgumentException::class.java) { SecaFramer().feed(ByteArray(0), 1) }
    }
    @Test fun configurationIsReadOnlyFromTypedFieldsAfterStatusAndSamples() {
        val config = "samplerate=4000\r\ngain=6\r\nmode=standby   \u0000"
        val text = config.toByteArray()
        val field = byteArrayOf(0x41, (text.size ushr 8).toByte(), text.size.toByte()) + text
        val status = byteArrayOf(0x50, 0x0f, 0x01, 87, 0x4d, 0, 0x4f, 0, 0x4b, 0, 0x53, 0, 0, 0, 0, 0, 0x73, 0, 0, 0, 0, 0)
        val widths = listOf(0 to 0, 4 to 16, 8 to 31, 12 to 48, 16 to 62, 20 to 78, 24 to 93)
        widths.forEach { (width, count) ->
            val packet = rx(status + byteArrayOf(0xc1.toByte(), width.toByte(), 0, 0, 0) + ByteArray(count) + field)
            assertEquals(mapOf("samplerate" to "4000", "gain" to "6", "mode" to "standby"), SecaProtocol.configuration(packet))
        }
        listOf(byteArrayOf(0x41), byteArrayOf(0x41, 0, 10, 1), byteArrayOf(0xc1.toByte()), byteArrayOf(0xc1.toByte(), 3), byteArrayOf(0x22)).forEach {
            assertTrue(SecaProtocol.configuration(rx(it)).isEmpty())
        }
    }
    @Test fun malformedFrameNeverDropsLaterFramesFromTheSameSocketRead() {
        val good = rx(byteArrayOf(0x4d, 0))
        val bad = good.copyOf().also { it[it.lastIndex] = (it.last().toInt() xor 1).toByte() }
        val frames = SecaFramer().feed(bad + good + good)
        val retained = mutableListOf<ByteArray>()
        assertThrows(java.io.IOException::class.java) { SecaProtocol.retainAndValidate(frames) { retained += it } }
        assertEquals(3, retained.size)
        assertArrayEquals(bad + good + good, retained.fold(ByteArray(0)) { all, frame -> all + frame })
        retained.clear()
        SecaProtocol.retainAndValidate(listOf(good)) { retained += it }
        assertArrayEquals(good, retained.single())
        retained.clear()
        assertThrows(java.io.IOException::class.java) { SecaProtocol.retainAndValidate(listOf(ByteArray(70000))) { retained += it } }
        assertEquals(listOf(65535, 4465), retained.map { it.size })
    }
    @Test fun diagnosticContainsOnlyStructuralNumbersAndHandlesShortFrames() {
        val frame = rx(byteArrayOf(0x4d, 0))
        val description = SecaProtocol.diagnostic(frame)
        assertTrue(description.contains("size=13 magic=true declared=13"))
        assertTrue(description.contains("counter=0"))
        assertEquals("size=0 magic=false declared=null checksum_expected=null checksum_actual=null counter=null", SecaProtocol.diagnostic(byteArrayOf()))
        assertTrue(SecaProtocol.diagnostic(byteArrayOf(1, 2)).contains("declared=null"))
    }
    companion object {
        fun rx(body: ByteArray): ByteArray {
            val frame = ByteArray(body.size + 11)
            frame[0] = 0x55; frame[1] = 0xaa.toByte(); SecaProtocol.put16(frame, 2, frame.size)
            frame[8] = 0x20; body.copyInto(frame, 9)
            SecaProtocol.put16(frame, frame.size - 2, -frame.dropLast(2).sumOf { it.toInt() and 255 })
            return frame
        }
    }
}
