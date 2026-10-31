package com.vineyardfinance.sensors.seca

import com.google.gson.GsonBuilder
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.Base64
import java.util.UUID

data class SecaRecord(val unix_ns: Long, val monotonic_ns: Long, val direction: String, val payload_b64: String, val acquisition: Boolean)
data class SecaBatch(
    val schema_version: Int = 1,
    val installation_id: String,
    val session_id: String,
    val batch_index: Long,
    val device_id: String,
    val sample_rate_hz: Int,
    val transport: String = "bluetooth",
    val gain: Int = 6,
    val contact_detection: Boolean = false,
    val end_reason: String? = null,
    val records: List<SecaRecord> = emptyList(),
)

/** Each journal line is synced before append returns. Chunks are immutable until verified ACK. */
class SecaQueue(private val directory: File, private val operationTiming: (String, Long) -> Unit = { _, _ -> }, private val syncDirectory: (File) -> Unit) {
    private val gson = GsonBuilder().serializeNulls().disableHtmlEscaping().create()
    init {
        if (!directory.isDirectory) {
            check(directory.mkdirs()) { "Cannot create seca archive queue" }
            directory.parentFile?.let(syncDirectory)
        }
    }
    @Synchronized fun pending(): List<File> = files().filter { it.name.endsWith(".json") && !File(directory, it.name.removeSuffix(".json") + ".journal").exists() }
        .sortedWith(compareBy<File> { it.name.substringBefore('_') }.thenBy { it.name })
    @Synchronized fun hasUnuploadedData(): Boolean = files().any { it.name.endsWith(".json") || it.name.endsWith(".journal") }
    @Synchronized fun sizeBytes(): Long = files().sumOf { it.length() }
    /** Explicit local diagnostic export; originals and upload ACK state are untouched. */
    @Synchronized fun exportChunks(destination: File): Int {
        require(!destination.exists()) { "seca export destination already exists" }
        val chunks = pending()
        check(chunks.sumOf { it.length() } <= 256L * 1024 * 1024) { "seca export exceeds 256 MiB; upload or export a smaller queue first" }
        check(destination.mkdirs()) { "Cannot create seca export directory" }
        chunks.forEach { source ->
            val target = File(destination, source.name)
            Files.copy(source.toPath(), target.toPath())
            // Android's copy preserves the private queue file's 0600 mode. The
            // explicitly exported copy needs read access for the authorized USB
            // shell; its enclosing scoped app directory remains access-controlled.
            check(target.setReadable(true, false)) { "Cannot make seca export readable over USB" }
            check(sha(target.readBytes()) == sha(source.readBytes())) { "seca export verification failed" }
        }
        return chunks.size
    }
    private fun files(): List<File> = directory.listFiles()?.toList() ?: throw IOException("Cannot list seca queue")
    private fun stem(batch: SecaBatch) = "${batch.session_id}_${batch.batch_index.toString().padStart(10, '0')}"
    private fun <T> timed(name: String, action: () -> T): T {
        val start = System.nanoTime()
        try { return action() } finally { operationTiming(name, System.nanoTime() - start) }
    }

    @Synchronized fun recover(): Int {
        var recovered = 0
        files().filter { it.name.endsWith(".journal") }.sortedBy { it.name }.forEach { journal ->
            val target = File(directory, journal.name.removeSuffix(".journal") + ".json")
            if (target.isFile) {
                // rename/fsync finished before the process died; never rewrite an ACK-able body.
                check(journal.delete()) { "Cannot remove sealed seca journal" }; syncDirectory(directory)
            } else {
                val lines = journal.readText().split('\n')
                check(lines.size >= 2) { "Invalid seca journal header: ${journal.name}" }
                val header = gson.fromJson(lines.first(), SecaBatch::class.java)
                val complete = lines.drop(1).dropLast(1).filter { it.isNotBlank() }
                val records = complete.map { gson.fromJson(it, SecaRecord::class.java) }
                // A partially written final line stays in the retained .interrupted evidence file.
                val interrupted = File(directory, journal.name + ".interrupted")
                Files.copy(journal.toPath(), interrupted.toPath(), StandardCopyOption.REPLACE_EXISTING)
                FileOutputStream(interrupted, true).use { it.fd.sync() }
                syncDirectory(directory)
                seal(header.copy(records = records, end_reason = "process_interrupted"), journal)
                recovered++
            }
        }
        return recovered
    }

    @Synchronized fun begin(batch: SecaBatch): Journal {
        require(batch.records.isEmpty() && batch.end_reason == null && batch.batch_index >= 0)
        UUID.fromString(batch.session_id); UUID.fromString(batch.installation_id)
        require(SecaProtocol.rates.contains(batch.sample_rate_hz) && batch.gain == 6)
        if (directory.usableSpace < 32L * 1024 * 1024) throw IOException("seca recording stopped: less than 32 MiB storage free")
        val file = File(directory, stem(batch) + ".journal")
        check(file.createNewFile()) { "seca journal already exists" }
        FileOutputStream(file).use { it.write((gson.toJson(batch) + "\n").toByteArray()); it.fd.sync() }
        syncDirectory(directory)
        return Journal(batch, file)
    }

    inner class Journal internal constructor(private val header: SecaBatch, private val file: File) {
        private val records = mutableListOf<SecaRecord>()
        private var encodedBytes = 0
        var closed = false; private set
        val count get() = records.size
        fun shouldSeal() = count >= 256 || encodedBytes >= 768 * 1024
        @Synchronized fun append(bytes: ByteArray, direction: String, unixNs: Long, monotonicNs: Long, acquisition: Boolean = false) {
            check(!closed)
            require(bytes.isNotEmpty() && bytes.size <= 65535 && direction in listOf("rx", "tx"))
            val record = SecaRecord(unixNs, monotonicNs, direction, Base64.getEncoder().encodeToString(bytes), acquisition && direction == "rx")
            val line = (gson.toJson(record) + "\n").toByteArray(Charsets.UTF_8)
            val output = timed("journal.open") { FileOutputStream(file, true) }
            try {
                timed("journal.write") { output.write(line) }
                timed("journal.fsync") { output.fd.sync() }
            } finally { timed("journal.close") { output.close() } }
            records += record; encodedBytes += line.size
        }
        @Synchronized fun finish(reason: String? = null): File {
            check(!closed)
            require(records.isNotEmpty() || reason != null)
            val result = seal(header.copy(records = records.toList(), end_reason = reason?.take(200)), file)
            closed = true
            return result
        }
    }

    @Synchronized private fun seal(batch: SecaBatch, journal: File): File = timed("chunk.seal") {
        val bytes = gson.toJson(batch).toByteArray(Charsets.UTF_8)
        check(bytes.size <= 2 * 1024 * 1024) { "seca batch exceeded request limit" }
        val target = File(directory, stem(batch) + ".json")
        val temp = File(directory, stem(batch) + ".tmp")
        FileOutputStream(temp).use { it.write(bytes); it.fd.sync() }
        Files.move(temp.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE)
        syncDirectory(directory)
        check(journal.delete()) { "Cannot remove sealed seca journal" }
        syncDirectory(directory)
        target
    }

    @Synchronized fun acknowledge(file: File, session: String, index: Long, sha256: String) {
        require(file.parentFile == directory && file.name.endsWith(".json"))
        val bytes = file.readBytes()
        val batch = gson.fromJson(String(bytes, Charsets.UTF_8), SecaBatch::class.java)
        check(batch.session_id == session && batch.batch_index == index && sha(bytes) == sha256) { "seca cloud acknowledgement mismatch; retained locally" }
        check(file.delete()) { "Cannot remove acknowledged seca batch" }
        syncDirectory(directory)
    }

    companion object {
        fun sha(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it.toInt() and 255) }
    }
}
