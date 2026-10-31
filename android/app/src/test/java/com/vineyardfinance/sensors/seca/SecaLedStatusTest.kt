package com.vineyardfinance.sensors.seca

import java.io.IOException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SecaLedStatusTest {
    private fun frame(body: ByteArray): ByteArray = ByteArray(body.size + 11).also {
        it[0] = 0x55; it[1] = 0xaa.toByte(); SecaProtocol.put16(it, 2, it.size)
        it[8] = 0x20; body.copyInto(it, 9)
        SecaProtocol.put16(it, it.size - 2, -it.dropLast(2).sumOf { byte -> byte.toInt() and 255 })
    }
    private fun status(raw: Int) = frame(byteArrayOf(0x4f, raw.toByte()))
    private fun channel(id: Int, width: Int): ByteArray {
        val size = mapOf(0 to 0, 4 to 16, 8 to 31, 12 to 48, 16 to 62, 20 to 78, 24 to 93).getValue(width)
        return byteArrayOf(id.toByte(), width.toByte()) + ByteArray(3 + size) { 0x4f }
    }
    private fun group(width: Int) = listOf(0xc6, 0xc5, 0xc4, 0xc3, 0xc2, 0xcb, 0xca, 0xc1).fold(byteArrayOf()) { bytes, id -> bytes + channel(id, width) }
    private fun missing(block: () -> Unit) { try { block(); fail("expected missing/stale status") } catch (_: IOException) { } }

    @Test fun independentlyReviewedSingleBitAndSharedFaultVectors() {
        val expected = mapOf(0 to "000000000", 1 to "010000000", 2 to "001000000", 3 to "111111111", 4 to "000100000", 8 to "000010000", 16 to "000001000", 32 to "000000100", 64 to "000000010", 128 to "000000001", 255 to "111111111")
        expected.forEach { (raw, bank) -> assertEquals(bank, SecaLedStatus.payload(raw).substring(10, 19)) }
        assertEquals("ledstatus=0000000001111111110000000000000000", SecaLedStatus.payload(0))
    }

    @Test fun allRawBytesProduceComplementaryNineBitBanksAndDefaultOverride() {
        for (raw in 0..255) {
            val payload = SecaLedStatus.payload(raw)
            assertEquals(44, payload.length); assertTrue(payload.startsWith("ledstatus="))
            assertEquals("0".repeat(16), payload.takeLast(16))
            val first = payload.substring(10, 19); val second = payload.substring(19, 28)
            assertTrue(first.zip(second).all { (a, b) -> (a == '0' && b == '1') || (a == '1' && b == '0') })
            assertNotEquals("0".repeat(34), payload.drop(10))
            if (raw % 4 == 3) assertEquals("111111111", first)
            else assertEquals(Integer.bitCount(raw / 4) + if (raw % 4 == 0) 0 else 1, first.count { it == '1' })
            val command = SecaProtocol.command(payload); assertTrue(SecaProtocol.valid(command)); assertEquals(58, command.size)
            assertEquals(0x57.toByte(), command[9])
        }
        for (raw in listOf(-1, 256)) try { SecaLedStatus.payload(raw); fail("range accepted") } catch (_: IllegalArgumentException) { }
    }

    @Test fun walksTypedFieldsAndNeverTreatsConfigurationOrSamplesAsO() {
        val text = "unknown=O123".toByteArray()
        val config = byteArrayOf(0x41, 0, text.size.toByte()) + text
        val metadata = byteArrayOf(0x50, 0, 0, 10, 0x4d, 1, 0x4b, 1, 0x53, 0, 0, 0, 0, 0, 0x73, 0, 0, 0, 0, 0)
        for (width in listOf(0, 4, 8, 12, 16, 20, 24)) {
            assertNull(SecaLedStatus.rawStatus(frame(config + group(width))))
            assertEquals(128, SecaLedStatus.rawStatus(frame(config + metadata + byteArrayOf(0x4f, 1) + group(width) + byteArrayOf(0x4f, 128.toByte()))))
        }
    }

    @Test fun rejectsWholeMalformedFrameEvenIfAnOFieldPrecedesFailure() {
        val o = byteArrayOf(0x4f, 4)
        val malformed = listOf(byteArrayOf(0x41), byteArrayOf(0x41, 0, 9, 1), byteArrayOf(0x4f), byteArrayOf(0x50, 0), byteArrayOf(0x7f), byteArrayOf(0xc6.toByte()), byteArrayOf(0xc6.toByte(), 3), channel(0xc6, 0), channel(0xc1, 0), channel(0xc6, 0) + channel(0xc6, 0), channel(0xc5, 0) + channel(0xc5, 0), channel(0xc5, 0) + channel(0xc6, 0))
        malformed.forEach { assertNull(SecaLedStatus.rawStatus(frame(o + it))) }
        val invalid = status(4).also { it[it.lastIndex] = 0 }; assertNull(SecaLedStatus.rawStatus(invalid))
        val good = frame(o + group(12))
        for (size in good.indices) assertNull(SecaLedStatus.rawStatus(good.copyOf(size)))
    }

    @Test fun statusFreshnessAndConnectionStateCannotBeInventedOrRefreshedByBadFrames() {
        val state = SecaLedStatus(); missing { state.command { 0 } }
        state.observe(status(1), 100)
        assertEquals(SecaLedStatus.payload(1), state.command { 3100 }.payload)
        missing { state.command { 99 } }; missing { state.command { 3101 } }
        state.observe(frame(byteArrayOf(0x4f, 4, 0x7f)), 5000)
        missing { state.command { 5000 } }
        state.observe(status(128), 6000); assertEquals(SecaLedStatus.payload(128), state.command { 6000 }.payload)
        missing { SecaLedStatus().command { 6000 } }
        assertEquals(SecaHostCommand("", true), secaKeepaliveCommand(false, SecaLedStatus()) { 0 })
        assertEquals(SecaHostCommand(SecaLedStatus.payload(128), false, 6000), secaKeepaliveCommand(true, state) { 6000 })
    }

    @Test fun delayedArchiveAckMustRevalidateTheExactAlreadyJournaledObservation() {
        val state = SecaLedStatus(); state.observe(status(1), 100)
        val selected = secaKeepaliveCommand(true, state) { 2100 }
        selected.validateBeforeWrite(3100)
        // A fresh RX update during the simulated durable-ACK delay cannot freshen old TX bytes.
        state.observe(status(128), 4000)
        missing { selected.validateBeforeWrite(4000) }
        assertEquals(SecaLedStatus.payload(1), selected.payload)
        secaKeepaliveCommand(false, state) { 100_000 }.validateBeforeWrite(100_000)
        assertEquals(SecaLedStatus.payload(128), secaKeepaliveCommand(true, state) { 4000 }.payload)
    }

    @Test fun experimentalTrafficUsesSameTwoSecondCadenceAndCancelsBeforeNextWrite() = runTest {
        val state = SecaLedStatus(); state.observe(status(0), 0)
        val sent = mutableListOf<SecaHostCommand>()
        val job = launch { runSecaKeepalive {
            sent += secaKeepaliveCommand(true, state) { testScheduler.currentTime }
            state.observe(status(128), testScheduler.currentTime)
        } }
        runCurrent(); advanceTimeBy(1999); runCurrent(); assertTrue(sent.isEmpty())
        advanceTimeBy(1); runCurrent(); assertEquals(SecaLedStatus.payload(0), sent.single().payload)
        advanceTimeBy(2000); runCurrent(); assertEquals(SecaLedStatus.payload(128), sent.last().payload)
        job.cancel(); runCurrent(); advanceTimeBy(5000); runCurrent(); assertEquals(2, sent.size)
        assertTrue(sent.none { it.read })
    }
    @Test fun clockIsSampledAfterConcurrentObservationInsideTheMonitor() {
        val state = SecaLedStatus(); state.observe(status(1), 100)
        val clock = java.util.concurrent.atomic.AtomicLong(100)
        val entered = java.util.concurrent.CountDownLatch(1)
        val result = java.util.concurrent.atomic.AtomicReference<SecaHostCommand>()
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable>()
        val worker = Thread {
            entered.countDown()
            try {
                result.set(secaKeepaliveCommand(true, state) {
                    assertTrue("clock must be sampled under observation monitor", Thread.holdsLock(state))
                    clock.get()
                })
            } catch (error: Throwable) { failure.set(error) }
        }
        synchronized(state) {
            worker.start()
            assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS))
            // The caller has entered, but selection cannot acquire this monitor.
            // Publish a later observation before allowing selection to continue.
            state.observe(status(128), 101); clock.set(101)
        }
        worker.join(2000); assertFalse("selection thread must finish", worker.isAlive)
        failure.get()?.let { throw AssertionError("concurrent selection failed", it) }
        assertEquals(SecaHostCommand(SecaLedStatus.payload(128), false, 101), result.get())
        // The old pre-sampled value would fail, even though the new O is fresh.
        missing { result.get().validateBeforeWrite(100) }
        result.get().validateBeforeWrite(3101)
        missing { result.get().validateBeforeWrite(3102) }
    }

}
