package com.vineyardfinance.sensors.seca

import java.io.IOException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.Assert.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SecaIngressTest {
    private var now = 0L
    private fun ingress(entries: Int = 128, bytes: Int = 512 * 1024) = SecaIngress({ now++; now to now }, entries, bytes)
    private val frame get() = SecaProtocol.command("hello")
    private fun failure(block: () -> Unit) { try { block(); fail("expected failure") } catch (_: IOException) { } }

    @Test fun everySplitCopiesAndPreservesPreRunPhase() = runTest {
        for (split in 1 until frame.size) {
            val pipe = ingress(); val input = frame
            pipe.receive(input, split, false); input.fill(0)
            val suffix = frame.copyOfRange(split, frame.size)
            pipe.receive(suffix, suffix.size, true); suffix.fill(0); pipe.end()
            val entry = pipe.next()!!
            assertArrayEquals(frame, entry.records.single().bytes); assertFalse(entry.records.single().acquisition)
            pipe.complete(entry); assertNull(pipe.next())
        }
    }

    @Test fun partialClockDoesNotReorderAnInterveningDurableTx() = runTest {
        val pipe = ingress(); pipe.receive(frame, 5, false)
        val tx = frame; val ack = pipe.transmit(tx); tx.fill(0); assertFalse(ack.isCompleted)
        pipe.receive(frame.copyOfRange(5, frame.size), frame.size - 5, true)
        pipe.receive(frame, frame.size, true); pipe.end()
        val first = pipe.next()!!; assertEquals("tx", first.records.single().direction)
        assertArrayEquals(frame, first.records.single().bytes); pipe.complete(first); ack.await()
        val second = pipe.next()!!; assertFalse(second.records.single().acquisition)
        assertTrue(second.records.single().monotonicNs > first.records.single().monotonicNs); pipe.complete(second)
        val third = pipe.next()!!; assertTrue(third.records.single().acquisition); pipe.complete(third); assertNull(pipe.next())
    }

    @Test fun slowConsumerDoesNotBlockReadsAndOverflowRetainsWholeReadPlusTail() = runTest {
        val pipe = ingress(entries = 1); pipe.receive(frame, frame.size, true)
        val first = pipe.next()!!
        pipe.receive(frame, frame.size, true)
        val last = frame + frame.copyOf(5)
        failure { pipe.receive(last, last.size, true) }; failure { pipe.transmit(frame) }
        assertTrue(pipe.diagnostic(now).contains("high_entries=3"))
        assertNotNull(pipe.stalled(2_000_000_100L)); pipe.end(); pipe.end(); pipe.complete(first)
        val saved = mutableListOf<Byte>()
        while (true) { val entry = pipe.next() ?: break; saved += entry.records.flatMap { it.bytes.toList() }; pipe.complete(entry) }
        assertArrayEquals(frame + last, saved.toByteArray()); assertNull(pipe.stalled(10_000_000_000L))
    }

    @Test fun byteLimitRejectsTxWithoutSendingAndRetainsInvalidCoalescedRx() = runTest {
        val pipe = ingress(bytes = 1); failure { pipe.transmit(frame) }; pipe.end(); assertNull(pipe.next())
        val broken = frame.also { it[it.lastIndex] = 0 }; val other = ingress()
        failure { other.receive(broken + frame, broken.size + frame.size, true) }; other.end()
        val entry = other.next()!!; assertEquals(2, entry.records.size)
        assertArrayEquals(broken + frame, entry.records.flatMap { it.bytes.toList() }.toByteArray())
        other.complete(entry); assertNull(other.next())
    }

    @Test fun cancellationAndDiskFailureNeverAcknowledgeUndurableTx() = runTest {
        val pipe = ingress(); val waiting = launch { pipe.next() }; runCurrent(); waiting.cancelAndJoin()
        val firstAck = pipe.transmit(frame); val nextAck = pipe.transmit(frame); pipe.next()!!
        assertNull(pipe.stalled(now)); pipe.abort(IOException("disk full")); pipe.end()
        assertTrue(firstAck.isCancelled); assertTrue(nextAck.isCancelled); failure { pipe.transmit(frame) }
        val fresh = ingress(); val waitingFresh = async { fresh.next() }; runCurrent()
        fresh.receive(frame, frame.size, true); fresh.end()
        val entry = waitingFresh.await()!!; assertTrue(entry.records.single().acquisition)
        fresh.complete(entry); assertNull(fresh.next())
    }

    @Test fun cancelledBeforeConsumerStartsCannotPassDrainBeforeSealGuard() = runTest {
        val pipe = ingress(); pipe.receive(frame, frame.size, true); pipe.end()
        val consumer = launch { pipe.next()?.let { pipe.complete(it) } }
        consumer.cancelAndJoin()
        assertFalse(pipe.drained())
        val entry = pipe.next()!!; assertFalse(pipe.drained())
        pipe.complete(entry); assertTrue(pipe.drained()); assertNull(pipe.next())
        assertTrue(pipe.diagnostic(now).contains("oldest_ms=0"))
    }

    @Test fun multipleFragmentsAndLargeMalformedTailRemainBoundedAndExact() = runTest {
        val pipe = ingress(); pipe.receive(frame, 2, true); pipe.receive(frame.copyOfRange(2, 4), 2, false)
        pipe.receive(frame.copyOfRange(4, frame.size) + frame, 2 * frame.size - 4, true); pipe.end()
        val entry = pipe.next()!!; assertFalse(entry.records[0].acquisition); assertTrue(entry.records[1].acquisition)
        pipe.complete(entry); assertNull(pipe.next())
        val large = ingress()
        val header = ByteArray(65534).also { it[0] = 0x55; it[1] = 0xaa.toByte(); SecaProtocol.put16(it, 2, 65535) }
        for (offset in header.indices step 8192) {
            val part = header.copyOfRange(offset, minOf(header.size, offset + 8192))
            large.receive(part, part.size, false)
        }; val rest = ByteArray(8192)
        failure { large.receive(rest, rest.size, true) }; large.end(); val retained = large.next()!!
        assertTrue(retained.records.all { it.bytes.size <= 65535 })
        assertArrayEquals(header + rest, retained.records.flatMap { it.bytes.toList() }.toByteArray())
        large.complete(retained); assertNull(large.next())
    }
}
