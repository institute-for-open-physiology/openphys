package com.vineyardfinance.sensors.seca

import java.io.IOException
import java.util.ArrayDeque
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel

/** Per-connection bounded RAM staging. Only the consumer touches the durable journal. */
class SecaIngress(
    private val clock: () -> Pair<Long, Long>,
    private val maxEntries: Int = 128,
    private val maxBytes: Int = 512 * 1024,
    recoveryRate: Int? = null,
) {
    data class Record(val bytes: ByteArray, val direction: String, val unixNs: Long, val monotonicNs: Long, val acquisition: Boolean)
    class Entry(val records: List<Record>, val durable: CompletableDeferred<Unit>? = null) {
        val bytes = records.sumOf { it.bytes.size }
    }
    private val queue = ArrayDeque<Entry>()
    private val changed = Channel<Unit>(Channel.CONFLATED)
    private val framer = SecaFramer()
    private val recovery = recoveryRate?.let { SecaRecovery(it) }
    private var pendingPhase = false
    private var pendingSize = 0
    private var closed = false
    private var ended = false
    private var receiverStopped = false
    private var bytes = 0
    private var highBytes = 0
    private var highEntries = 0
    private var inFlight: Entry? = null
    private var failure: Exception? = null

    /** Last-fragment acceptance clock; phase is false if any fragment preceded run confirmation. */
    @Synchronized fun receive(input: ByteArray, count: Int, acquisition: Boolean): List<ByteArray> {
        check(!ended && !receiverStopped) { "seca reader already ended" }
        require(count in 0..minOf(input.size, 8192))
        // Closed admission can race one final read: preserve it in the single reserved overflow
        // entry. The reader exits immediately after this call reports the closure.
        val previousSize = pendingSize
        val firstPhase = if (previousSize > 0) pendingPhase && acquisition else acquisition
        val recoveryStamp = recovery?.let { clock() }
        val result = recovery?.feed(input, count, firstPhase, requireNotNull(recoveryStamp).second)
        val pieces = result?.pieces ?: framer.feed(input, count).map { SecaRecovery.Piece(it, true) }
        pendingSize = recovery?.remaining()?.size ?: framer.remaining().size
        pendingPhase = if (pieces.isEmpty() && previousSize > 0) firstPhase else acquisition
        if (pieces.isNotEmpty()) {
            // Recovered pairs use confirmation-admission time, never an earlier
            // candidate clock that would reorder an intervening durable TX.
            val stamp = recoveryStamp ?: clock()
            val records = pieces.flatMapIndexed { index, piece ->
                val frame = piece.bytes
                (frame.indices step 65535).map { offset -> Record(frame.copyOfRange(offset, minOf(frame.size, offset + 65535)), "rx", stamp.first, stamp.second, piece.accepted && if (index == 0) firstPhase else acquisition) }
            }
            enqueue(Entry(records), preserve = true)
        }
        failure?.let { receiverStopped = true; throw it }
        val invalid = if (recovery == null) pieces.firstOrNull { !SecaProtocol.valid(it.bytes) } else null
        val problem = result?.failure ?: invalid?.let { "seca frame/checksum failure; raw bytes queued; ${SecaProtocol.diagnostic(it.bytes)}" }
        if (problem != null) {
            val error = IOException(problem)
            closeAdmission(error)
            receiverStopped = true
            throw error
        }
        return pieces.filter { it.accepted }.map { it.bytes }
    }

    @Synchronized fun transmit(payload: ByteArray): CompletableDeferred<Unit> {
        if (closed) throw failure ?: IOException("seca archive admission closed")
        val stamp = clock()
        val ack = CompletableDeferred<Unit>()
        enqueue(Entry(listOf(Record(payload.copyOf(), "tx", stamp.first, stamp.second, false)), ack), preserve = false)
        return ack
    }

    private fun enqueue(entry: Entry, preserve: Boolean) {
        if (closed || queue.size >= maxEntries || bytes + entry.bytes > maxBytes) {
            val error = failure ?: IOException("seca archive backlog exceeded $maxEntries entries/$maxBytes bytes")
            closeAdmission(error)
            if (!preserve) throw error
        }
        queue.addLast(entry); bytes += entry.bytes; measureBacklog()
        changed.trySend(Unit)
    }

    @Synchronized private fun closeAdmission(error: Exception) { failure = failure ?: error; closed = true; changed.trySend(Unit) }

    /** Called once, only after socket reader ownership ends; retain its partial frame too. */
    @Synchronized fun end() {
        if (ended) return
        val tails = recovery?.finish() ?: listOf(SecaRecovery.Piece(framer.remaining(), true))
        for (piece in tails.filter { it.bytes.isNotEmpty() }) {
            val stamp = clock()
            val records = (piece.bytes.indices step 65535).map { offset ->
                Record(piece.bytes.copyOfRange(offset, minOf(piece.bytes.size, offset + 65535)), "rx", stamp.first, stamp.second, pendingPhase && piece.accepted)
            }
            queue.addLast(Entry(records))
            bytes += piece.bytes.size; measureBacklog()
        }
        pendingSize = 0
        ended = true; closed = true; changed.trySend(Unit)
    }

    suspend fun next(): Entry? {
        while (true) {
            synchronized(this) {
                check(inFlight == null) { "seca archive entry not completed" }
                if (queue.isNotEmpty()) return queue.removeFirst().also { bytes -= it.bytes; inFlight = it }
                if (ended) return null
            }
            changed.receive()
        }
    }

    @Synchronized fun complete(entry: Entry) {
        check(inFlight === entry)
        inFlight = null
        entry.durable?.complete(Unit)
    }

    /** Fsync failure cannot be repaired by dropping queued bytes or reconnecting. */
    @Synchronized fun abort(error: Exception) {
        closeAdmission(error)
        inFlight?.durable?.completeExceptionally(error)
        queue.forEach { it.durable?.completeExceptionally(error) }
    }

    private fun measureBacklog() {
        highBytes = maxOf(highBytes, bytes + (inFlight?.bytes ?: 0))
        highEntries = maxOf(highEntries, queue.size + if (inFlight == null) 0 else 1)
    }

    /** Backlog includes in-flight work, which may already be partly durable: an upper bound. */
    @Synchronized fun diagnostic(nowNs: Long): String {
        val oldest = inFlight ?: queue.peekFirst()
        val ageMs = oldest?.let { maxOf(0L, nowNs - it.records.first().monotonicNs) / 1_000_000 } ?: 0
        return "ram_bytes=${bytes + (inFlight?.bytes ?: 0)} ram_entries=${queue.size + if (inFlight == null) 0 else 1} partial_bytes=$pendingSize high_bytes=$highBytes high_entries=$highEntries oldest_ms=$ageMs"
    }

    @Synchronized fun recoveryStats(): SecaRecovery.Stats? = recovery?.stats()
    @Synchronized fun recoveryTimeout(nowNs: Long): String? = recovery?.timeout(nowNs)

    @Synchronized fun drained(): Boolean = ended && queue.isEmpty() && inFlight == null

    @Synchronized fun stalled(nowNs: Long): String? {
        val oldest = inFlight ?: queue.peekFirst() ?: return null
        return if (nowNs - oldest.records.first().monotonicNs > 2_000_000_000L) {
            "seca archive stalled; ${diagnostic(nowNs)}"
        } else null
    }
}
