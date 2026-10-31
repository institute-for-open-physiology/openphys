package com.vineyardfinance.sensors.seca

import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.io.IOException
import java.util.UUID

class SecaQueueTest {
    @get:Rule val temp = TemporaryFolder()
    private fun header(index: Long = 0, session: String = UUID.randomUUID().toString()) = SecaBatch(installation_id = UUID.randomUUID().toString(), session_id = session, batch_index = index, device_id = "AA:BB:CC:DD:EE:FF", sample_rate_hz = 4000)
    private fun SecaQueue.Journal.add() = append(byteArrayOf(0x55, 0xaa.toByte()), "rx", 1_700_000_000_000_000_000, 123)

    @Test fun journalAndBatchAreDurableBeforeVerifiedAcknowledgementDeletes() {
        var syncs = 0
        val queue = SecaQueue(File(temp.root, "queue")) { syncs++ }
        assertFalse(queue.hasUnuploadedData())
        val h = header(); val journal = queue.begin(h)
        assertTrue(queue.hasUnuploadedData())
        assertEquals(0, journal.count); assertFalse(journal.shouldSeal())
        journal.add(); assertEquals(1, journal.count)
        assertTrue(queue.sizeBytes() > 0)
        assertTrue(queue.pending().isEmpty())
        val file = journal.finish(); assertTrue(journal.closed)
        assertEquals(listOf(file), queue.pending())
        assertTrue(queue.hasUnuploadedData())
        assertTrue(syncs >= 4)
        val body = file.readText(); val batch = Gson().fromJson(body, SecaBatch::class.java)
        assertEquals("bluetooth", batch.transport); assertEquals(6, batch.gain)
        assertEquals("Vao=", batch.records.single().payload_b64)
        assertFalse(batch.records.single().acquisition)
        assertThrows(IllegalStateException::class.java) { queue.acknowledge(file, h.session_id, 0, "wrong") }
        assertThrows(IllegalStateException::class.java) { queue.acknowledge(file, UUID.randomUUID().toString(), 0, SecaQueue.sha(file.readBytes())) }
        assertThrows(IllegalStateException::class.java) { queue.acknowledge(file, h.session_id, 1, SecaQueue.sha(file.readBytes())) }
        assertTrue(file.isFile)
        queue.acknowledge(file, h.session_id, 0, SecaQueue.sha(file.readBytes()))
        assertTrue(queue.pending().isEmpty())
        assertFalse(queue.hasUnuploadedData())
        assertThrows(IllegalStateException::class.java) { journal.add() }
        assertThrows(IllegalStateException::class.java) { journal.finish() }
    }
    @Test fun incompleteJournalRecoversCommittedLinesAndRetainsTornTailEvidence() {
        val directory = temp.newFolder(); val queue = SecaQueue(directory) {}
        val h = header(); queue.begin(h).add()
        val journalFile = directory.listFiles()!!.single()
        journalFile.appendText("{partial")
        val restarted = SecaQueue(directory) {}
        assertEquals(1, restarted.recover())
        val batch = Gson().fromJson(restarted.pending().single().readText(), SecaBatch::class.java)
        assertEquals("process_interrupted", batch.end_reason)
        assertEquals(1, batch.records.size)
        assertTrue(directory.listFiles()!!.single { it.name.endsWith(".interrupted") }.readText().endsWith("{partial"))
        assertEquals(0, restarted.recover())
    }
    @Test fun crashedSealDoesNotUploadUntilJournalCleanupAndNeverRewritesSealedBody() {
        val directory = temp.newFolder(); var fail = false
        val queue = SecaQueue(directory) { if (fail) throw IOException("directory sync failed") }
        val journal = queue.begin(header()); journal.add(); fail = true
        assertThrows(IOException::class.java) { journal.finish() }
        assertTrue(queue.pending().isEmpty())
        val bytes = directory.listFiles()!!.single { it.name.endsWith(".json") }.readBytes()
        val restarted = SecaQueue(directory) {}
        assertEquals(0, restarted.recover())
        assertArrayEquals(bytes, restarted.pending().single().readBytes())
    }
    @Test fun emptyEndBatchLimitsAndSessionOrderingAreExplicit() {
        val queue = SecaQueue(temp.newFolder()) {}
        val h = header()
        val first = queue.begin(h)
        assertThrows(IllegalArgumentException::class.java) { first.finish() }
        assertThrows(IllegalArgumentException::class.java) { first.append(byteArrayOf(), "rx", 1, 1) }
        assertThrows(IllegalArgumentException::class.java) { first.append(byteArrayOf(1), "bad", 1, 1) }
        repeat(256) { first.add() }; assertTrue(first.shouldSeal()); first.finish()
        queue.begin(h.copy(batch_index = 1)).finish("user_stop")
        assertEquals(listOf(0L, 1L), queue.pending().map { Gson().fromJson(it.readText(), SecaBatch::class.java).batch_index })
        assertThrows(IllegalArgumentException::class.java) { queue.begin(h.copy(sample_rate_hz = 32000)) }
        assertThrows(IllegalArgumentException::class.java) { queue.begin(h.copy(batch_index = -1)) }
        assertThrows(IllegalArgumentException::class.java) { queue.begin(h.copy(session_id = "bad")) }
        val open = queue.begin(h.copy(batch_index = 2)); open.add()
        assertThrows(IllegalStateException::class.java) { queue.begin(h.copy(batch_index = 2)) }
        queue.recover()
        assertEquals(3, queue.pending().size)
    }
    @Test fun acquisitionBoundarySurvivesCrashRecovery() {
        val directory = temp.newFolder(); val queue = SecaQueue(directory) {}
        val journal = queue.begin(header())
        journal.append(byteArrayOf(1), "rx", 1, 1, acquisition = false)
        journal.append(SecaProtocol.command("", read = true), "tx", 2, 2, acquisition = true)
        journal.append(byteArrayOf(2), "rx", 3, 3, acquisition = true)
        queue.recover()
        val batch = Gson().fromJson(queue.pending().single().readText(), SecaBatch::class.java)
        assertEquals(listOf(false, false, true), batch.records.map { it.acquisition })
    }
    @Test fun explicitExportCopiesOnlyImmutableChunksWithoutRemovingQueuedData() {
        val directory = temp.newFolder(); val queue = SecaQueue(directory) {}
        val h = header(); val first = queue.begin(h); first.add(); val file = first.finish()
        queue.begin(h.copy(batch_index = 1)).add()
        val export = File(temp.root, "exports/session")
        assertEquals(1, queue.exportChunks(export))
        assertArrayEquals(file.readBytes(), File(export, file.name).readBytes())
        assertEquals(listOf(file), queue.pending())
        assertEquals(2, directory.listFiles()!!.size)
        assertThrows(IllegalArgumentException::class.java) { queue.exportChunks(export) }
    }
}
