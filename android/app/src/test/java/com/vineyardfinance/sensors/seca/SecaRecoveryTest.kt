package com.vineyardfinance.sensors.seca

import com.google.gson.JsonParser
import java.io.File
import java.util.Base64
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Assume.assumeTrue
import org.junit.Test

class SecaRecoveryTest {
    companion object {
        fun frame(body: ByteArray, counter: Long): ByteArray = ByteArray(body.size + 11).also {
            it[0] = 0x55; it[1] = 0xaa.toByte(); SecaProtocol.put16(it, 2, it.size)
            for (i in 0..3) it[4 + i] = (counter ushr (24 - 8 * i)).toByte()
            body.copyInto(it, 9); SecaProtocol.put16(it, it.size - 2, -it.dropLast(2).sumOf { b -> b.toInt() and 255 })
        }
        fun group(width: Int = 0): ByteArray {
            val n = when (width) { 0 -> 0; 4 -> 16; 8 -> 31; 12 -> 48; 16 -> 62; 20 -> 78; else -> 93 }
            return listOf(0xc6, 0xc5, 0xc4, 0xc3, 0xc2, 0xcb, 0xca, 0xc1).flatMap {
                (byteArrayOf(it.toByte(), width.toByte(), 0, 0, 0) + ByteArray(n)).toList()
            }.toByteArray()
        }
        fun ecg(counter: Long, groups: Int = 1) = frame((1..groups).flatMap { group().toList() }.toByteArray(), counter)
        fun joined(pieces: List<SecaRecovery.Piece>) = pieces.flatMap { it.bytes.toList() }.toByteArray()
    }
    private fun ready(counter: Long = 1): SecaRecovery = SecaRecovery(4000).also { assertNull(it.feed(ecg(counter), ecg(counter).size, true, 0).failure) }

    @Test fun everySplitRequiresTwoFramesAndConservesCorruptPrefix() {
        val input = byteArrayOf(9, 8, 7, 6) + ecg(7) + ecg(8, 1)
        for (split in 0..input.size) {
            val parser = ready(); val pieces = mutableListOf<SecaRecovery.Piece>()
            for (part in listOf(input.copyOf(split), input.copyOfRange(split, input.size))) {
                val result = parser.feed(part, part.size, true, 10); assertNull(result.failure); pieces += result.pieces
            }
            pieces += parser.finish()
            assertArrayEquals(input, joined(pieces)); assertEquals(4L, parser.stats().corruptBytes)
            assertEquals(1L, parser.stats().recovered); assertEquals(5L, parser.stats().missingGroups)
            assertEquals(listOf(7L, 8L), pieces.filter { it.accepted }.map { requireNotNull(SecaTypedFrame.inspect(it.bytes)).last })
            assertFalse(pieces.first().accepted)
        }
    }
    @Test fun twoKhzChecksumFailureRetainsBadFrameAndRecoversAcrossEverySplit() {
        val baseline = ecg(72407)
        // Complete header/length with a damaged checksum, as in the incident.
        val bad = frame(group(4), 72408).also { it[it.lastIndex] = (it.last().toInt() xor 1).toByte() }
        val input = bad + ecg(72409) + ecg(72410)
        for (split in 0..input.size) {
            val parser = SecaRecovery(2000)
            assertNull(parser.feed(baseline, baseline.size, true, 0).failure)
            val pieces = mutableListOf<SecaRecovery.Piece>()
            for (part in listOf(input.copyOf(split), input.copyOfRange(split, input.size))) {
                val result = parser.feed(part, part.size, true, 1)
                assertNull(result.failure); pieces += result.pieces
            }
            pieces += parser.finish()
            assertArrayEquals(input, joined(pieces))
            assertArrayEquals(bad, pieces.first().bytes)
            assertFalse(pieces.first().accepted)
            assertEquals(listOf(72409L, 72410L), pieces.filter { it.accepted }.map { SecaTypedFrame.inspect(it.bytes)?.last })
            assertEquals(1L, parser.stats().episodes)
            assertEquals(1L, parser.stats().recovered)
            assertEquals(1L, parser.stats().missingGroups)
            assertEquals(bad.size.toLong(), parser.stats().corruptBytes)
        }
    }
    @Test fun bogusIncompleteLengthAndRejectedCountersDoNotHideLaterPair() {
        val parser = ready(100)
        val fake = byteArrayOf(0, 0x55, 0xaa.toByte(), 0xff.toByte(), 0xff.toByte())
        val rejected = ecg(99) + ecg(100) + ecg(105)
        val input = fake + rejected + ecg(107) + ecg(108)
        val result = parser.feed(input, input.size, true, 0)
        assertNull(result.failure); assertArrayEquals(input, joined(result.pieces))
        assertArrayEquals(fake + rejected, result.pieces.first().bytes); assertFalse(result.pieces.first().accepted)
        assertEquals(listOf(107L,108L), result.pieces.filter { it.accepted }.map { SecaTypedFrame.inspect(it.bytes)?.last })
        assertEquals(6L, parser.stats().missingGroups)
    }
    @Test fun counterWrapAllowedButHugeForwardJumpAndStatusOnlyCannotConfirm() {
        val parser = ready(0xffff_fffeL)
        val input = byteArrayOf(0) + ecg(0xffff_ffffL) + ecg(0)
        assertNull(parser.feed(input, input.size, true, 0).failure); assertEquals(0L, parser.stats().missingGroups)
        val stopped = ready()
        val rejected = byteArrayOf(0) + ecg(4000) + ecg(4001) + frame(byteArrayOf(0x4f, 0), 1)
        assertTrue(stopped.feed(rejected, rejected.size, true, 0).pieces.isEmpty())
        assertEquals(0L, stopped.stats().recovered); assertFalse(stopped.finish().single().accepted)
    }
    @Test fun stopAndDeadlineRetainUnconfirmedCandidateExactlyOnce() {
        val partial = byteArrayOf(0) + ecg(3) + ecg(4).copyOf(12)
        val parser = ready(); assertTrue(parser.feed(partial, partial.size, true, 10).pieces.isEmpty())
        assertNull(parser.timeout(2_000_000_010)); assertNotNull(parser.timeout(2_000_000_011))
        assertArrayEquals(partial, joined(parser.finish())); assertTrue(parser.finish().isEmpty())
        val timed = ready(); timed.feed(partial, partial.size, true, 0)
        val suffix = ecg(4).copyOfRange(12, ecg(4).size)
        val result = timed.feed(suffix, suffix.size, true, 2_000_000_001)
        assertTrue(result.failure.orEmpty().contains("two seconds")); assertArrayEquals(partial + suffix, joined(result.pieces))
        assertTrue(result.pieces.none { it.accepted }); assertTrue(timed.remaining().isEmpty())
    }
    @Test fun setupAndMissingBaselineRemainFailClosed() {
        for (phase in listOf(false, true)) {
            val parser = SecaRecovery(4000); val input = byteArrayOf(1,2,3,4) + ecg(1) + ecg(2)
            val result = parser.feed(input, input.size, phase, 0)
            assertNotNull(result.failure); assertArrayEquals(input, joined(result.pieces)); assertTrue(result.pieces.none { it.accepted })
        }
    }
    @Test fun normalIncompleteFrameNeverScansMagicInsideValidSamples() {
        val channels = group(24); ecg(10).copyInto(channels, 8)
        val embedded = ecg(10) + ecg(11)
        val body = byteArrayOf(0x41,0,embedded.size.toByte()) + embedded + channels
        val input = frame(body, 2); val parser = ready()
        assertTrue(parser.feed(input, input.size - 1, true, 0).pieces.isEmpty())
        val result = parser.feed(input.takeLast(1).toByteArray(), 1, true, 1)
        assertNull(result.failure); assertArrayEquals(input, result.pieces.single().bytes); assertEquals(0L, parser.stats().episodes)
    }
    @Test fun typedValidatorChecksAllFieldsWidthsAndWholeGroupCompletion() {
        val metadata = byteArrayOf(0x41,0,2,65,66,0x4f,0,0x4d,0,0x4b,0,0x50,0,0,0,0x53,0,0,0,0,0,0x73,0,0,0,0,0)
        assertEquals(0, SecaTypedFrame.inspect(frame(metadata, 0))?.groups)
        for (width in listOf(0,4,8,12,16,20,24)) assertEquals(2, SecaTypedFrame.inspect(frame(metadata + group(width) + group(width), 4))?.groups)
        val bad = listOf(byteArrayOf(0x41),byteArrayOf(0x41,0,12),byteArrayOf(0x4f),byteArrayOf(0x70),byteArrayOf(0xc6.toByte()),
            byteArrayOf(0xc6.toByte(),3), group().copyOf(5), group().copyOf(5) + group(), group().copyOfRange(5,40),
            group().copyOfRange(5,10) + group().copyOfRange(5,10), byteArrayOf(0xc1.toByte(),0,0,0,0))
        bad.forEach { assertNull(SecaTypedFrame.inspect(frame(it, 1))) }
        assertNull(SecaTypedFrame.inspect(byteArrayOf()))
        val corrupt = ecg(1).also { it[it.lastIndex] = 0 }; assertNull(SecaTypedFrame.inspect(corrupt))
    }
    @Test fun finiteReadWorkAndBufferBudgetsRetainEverything() {
        val headers = byteArrayOf(0) + (1..40).flatMap { byteArrayOf(0x55,0xaa.toByte(),0,11,0,0,0,0,0,0,0).toList() }.toByteArray()
        val work = ready(); val bad = work.feed(headers, headers.size, true, 0)
        assertTrue(bad.failure.orEmpty().contains("32 candidate")); assertArrayEquals(headers, joined(bad.pieces))
        val large = ready(); val output = mutableListOf<SecaRecovery.Piece>(); var total = byteArrayOf()
        repeat(9) {
            val bytes = ByteArray(8192); total += bytes
            val result = large.feed(bytes, bytes.size, true, 0); output += result.pieces
            if (it < 8) assertNull(result.failure) else assertTrue(result.failure.orEmpty().contains("64 KiB"))
        }
        assertArrayEquals(total, joined(output)); assertTrue(output.none { it.accepted })
        val many = ready(); var last: SecaRecovery.Result? = null; var all = byteArrayOf()
        repeat(257) {
            val bytes = byteArrayOf(0,0x55,0xaa.toByte(),0xff.toByte(),0xff.toByte()); all += bytes
            last = many.feed(bytes, bytes.size, true, 0)
        }
        assertTrue(last?.failure.orEmpty().contains("256 candidate headers")); assertArrayEquals(all, joined(requireNotNull(last).pieces))
    }
    @Test fun ingressPreservesTxOrderAndSuppressesUnconfirmedWaveform() = runTest {
        var now = 0L
        val pipe = SecaIngress({ ++now to now }, recoveryRate = 4000)
        pipe.receive(ecg(1), ecg(1).size, true)
        val waiting = byteArrayOf(0) + ecg(3)
        pipe.receive(waiting, waiting.size, true)
        val ack = pipe.transmit(SecaProtocol.command("ledstatus=test"))
        pipe.receive(ecg(4), ecg(4).size, true); pipe.end()
        val records = mutableListOf<SecaIngress.Record>()
        while (true) { val e = pipe.next() ?: break; records += e.records; pipe.complete(e) }
        ack.await(); assertTrue(pipe.drained())
        assertEquals(listOf("rx","tx","rx","rx","rx"), records.map { it.direction })
        assertFalse(records[2].acquisition); assertTrue(records[3].acquisition)
        assertTrue(records.zipWithNext().all { (a,b) -> a.monotonicNs <= b.monotonicNs })
        assertArrayEquals(ecg(1) + waiting + ecg(4), records.filter { it.direction == "rx" }.flatMap { it.bytes.toList() }.toByteArray())
    }
    @Test fun finiteValidationBudgetAndRegressionTriggerRetainRawBytes() {
        val parser = ready()
        val input = ByteArray(24576) { 42 }
        for (i in 1..31) {
            val at = i * 100; input[at] = 0x55; input[at+1] = 0xaa.toByte(); SecaProtocol.put16(input,at+2,20000)
        }
        val output = mutableListOf<SecaRecovery.Piece>()
        for (at in input.indices step 8192) {
            val part = input.copyOfRange(at,at+8192); val result = parser.feed(part,part.size,true,0); output += result.pieces
            if (at == 16384) assertTrue(result.failure.orEmpty().contains("256 KiB validation")) else assertNull(result.failure)
        }
        assertArrayEquals(input,joined(output)); assertTrue(output.none { it.accepted })
        val regression = ready(100)
        val reset = ecg(1) + ecg(2)
        assertTrue(regression.feed(reset,reset.size,true,0).pieces.isEmpty())
        assertEquals(1L,regression.stats().episodes); assertArrayEquals(reset,joined(regression.finish()))
    }
    @Test fun recoveryOverflowAndShutdownConserveLargeUnconfirmedTail() = runTest {
        var now = 0L
        val pipe = SecaIngress({ ++now to now }, maxEntries = 1, recoveryRate = 4000)
        pipe.receive(ecg(1),ecg(1).size,true)
        val input = byteArrayOf(0) + ecg(3) + ecg(4)
        try { pipe.receive(input,input.size,true); fail("overflow expected") } catch (_: java.io.IOException) { }
        pipe.end()
        val records = mutableListOf<SecaIngress.Record>()
        while (true) { val e = pipe.next() ?: break; records += e.records; pipe.complete(e) }
        assertArrayEquals(ecg(1)+input,records.flatMap { it.bytes.toList() }.toByteArray()); assertTrue(pipe.drained())
        assertEquals(1L,pipe.recoveryStats()?.recovered); assertNull(pipe.recoveryTimeout(now))
        val large = SecaIngress({ ++now to now }, recoveryRate = 4000)
        large.receive(ecg(1),ecg(1).size,true)
        val all = ByteArray(9*8192)
        for (at in all.indices step 8192) {
            try { large.receive(all.copyOfRange(at,at+8192),8192,true) }
            catch (_: java.io.IOException) { assertEquals(8*8192,at) }
        }
        large.end(); val largeRecords = mutableListOf<SecaIngress.Record>()
        while (true) { val e = large.next() ?: break; largeRecords += e.records; large.complete(e) }
        assertTrue(largeRecords.all { it.bytes.size <= 65535 })
        assertArrayEquals(ecg(1)+all,largeRecords.flatMap { it.bytes.toList() }.toByteArray())
        assertTrue(largeRecords.drop(1).none { it.acquisition })
        val stopping = SecaIngress({ ++now to now }, recoveryRate=4000)
        stopping.receive(ecg(1),ecg(1).size,true)
        val pending = byteArrayOf(0) + ecg(3) + ecg(4).copyOf(7)
        stopping.receive(pending,pending.size,true); assertNotNull(stopping.recoveryTimeout(now+2_000_000_001))
        stopping.end(); val first = requireNotNull(stopping.next()); stopping.complete(first)
        val tail = requireNotNull(stopping.next()); assertArrayEquals(pending,tail.records.single().bytes)
        assertFalse(tail.records.single().acquisition); stopping.complete(tail); assertNull(stopping.next())
    }
    @Test fun privateHciReplayConservesEightStreamsAcrossEveryFatalSplit() {
        val path = System.getenv("SECA_RECOVERY_CASES")
        assumeTrue("private HCI fixture supplied only for local replay", path != null)
        val cases = JsonParser.parseString(File(requireNotNull(path)).readText()).asJsonArray
        for (case in cases) {
            val obj = case.asJsonObject
            val chunks = obj["chunks"].asJsonArray.map { Base64.getDecoder().decode(it.asJsonObject["payload_b64"].asString) }
            val input = chunks.flatMap { it.toList() }.toByteArray()
            val expected = obj["expected_accepted_frame_sha256"].asJsonArray.map { it.asString }
            // Every boundary around the fatal payload, plus each original HCI payload.
            val variants = (0..chunks[1].size).map { split -> listOf(chunks[0], chunks[1].copyOf(split), chunks[1].copyOfRange(split,chunks[1].size)) + chunks.drop(2) }
            for (parts in variants) {
                val parser = SecaRecovery(4000); val output = mutableListOf<SecaRecovery.Piece>()
                for (part in parts) {
                    for (at in part.indices step 8192) {
                        val bytes = part.copyOfRange(at,minOf(part.size,at+8192)); val result = parser.feed(bytes,bytes.size,true,0)
                        assertNull("${obj["case"]}",result.failure); output += result.pieces
                    }
                }
                output += parser.finish()
                assertArrayEquals(input,joined(output))
                assertEquals(expected,output.filter { it.accepted }.map { SecaQueue.sha(it.bytes) })
                assertEquals(obj["expected_corrupt_bytes"].asLong,parser.stats().corruptBytes)
                assertEquals(obj["expected_missing_groups"].asLong,parser.stats().missingGroups)
            }
        }
    }
}
