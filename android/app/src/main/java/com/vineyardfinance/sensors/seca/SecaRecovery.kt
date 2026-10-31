package com.vineyardfinance.sensors.seca

/** Whole typed validation without sample allocation; packet counter names its final group. */
object SecaTypedFrame {
    data class Info(val groups: Int, val last: Long) {
        val first get() = (last - groups + 1) and MASK
    }
    const val MASK = 0xffff_ffffL
    fun inspect(frame: ByteArray): Info? {
        if (!SecaProtocol.valid(frame)) return null
        var at = 9
        val end = frame.size - 2
        var channels = 0
        var groups = 0
        while (at < end) {
            val tag = frame[at++].toInt() and 255
            val size = when (tag) {
                0x41 -> {
                    if (at + 2 > end) return null
                    val length = SecaProtocol.u16(frame, at); at += 2; length
                }
                0x4f, 0x4d, 0x4b -> 1
                0x50 -> 3
                0x53, 0x73 -> 5
                in 0xc1..0xc6, 0xca, 0xcb -> {
                    if (at >= end || (tag == 0xc6 && channels != 0)) return null
                    val bit = 1 shl when (tag) { 0xca -> 6; 0xcb -> 7; else -> tag - 0xc1 }
                    if (channels and bit != 0) return null
                    channels = channels or bit
                    if (tag == 0xc1) {
                        if (channels != 255) return null
                        groups++; channels = 0
                    }
                    val packed = when (frame[at].toInt() and 255) {
                        0 -> 0; 4 -> 16; 8 -> 31; 12 -> 48; 16 -> 62; 20 -> 78; 24 -> 93
                        else -> return null
                    }
                    4 + packed
                }
                else -> return null
            }
            if (size > end - at) return null
            at += size
        }
        if (channels != 0) return null
        val counter = (4..7).fold(0L) { value, index -> (value shl 8) or (frame[index].toLong() and 255) }
        return Info(groups, counter)
    }
}

/** Bounded recovery: retain all bytes, confirm recovery with two adjacent ECG frames. */
class SecaRecovery(private val rate: Int) {
    data class Piece(val bytes: ByteArray, val accepted: Boolean)
    data class Result(val pieces: List<Piece>, val failure: String?)
    data class Stats(val episodes: Long, val recovered: Long, val corruptBytes: Long, val missingGroups: Long)
    private data class Candidate(val offset: Int, val size: Int, var checked: Boolean = false, var info: SecaTypedFrame.Info? = null)
    private var pending = ByteArray(0)
    private var lastCounter: Long? = null
    private var started: Long? = null
    private var fatal: String? = null
    private var scan = 0
    private var examined = 0
    private var validatedBytes = 0
    private var readWork = 0
    private val candidates = linkedMapOf<Int, Candidate>()
    private var episodes = 0L
    private var recovered = 0L
    private var corruptBytes = 0L
    private var missingGroups = 0L
    private val maxForward = rate / 16 + 2048L
    init { require(rate in SecaProtocol.rates) }
    fun stats() = Stats(episodes, recovered, corruptBytes, missingGroups)
    fun remaining() = pending.copyOf()
    fun timeout(nowNs: Long): String? = fatal ?: started?.let {
        if (nowNs - it > 2_000_000_000L) "seca recovery exceeded two seconds; raw bytes retained" else null
    }

    fun feed(input: ByteArray, count: Int, acquisition: Boolean, nowNs: Long): Result {
        require(count in 0..minOf(input.size, 8192))
        check(fatal == null) { "seca recovery already failed" }
        pending += input.copyOf(count)
        readWork = 0
        val output = mutableListOf<Piece>()
        while (pending.isNotEmpty()) {
            if (started != null) {
                if (pending.size > 65536) fail("64 KiB search buffer")
                timeout(nowNs)?.let { fatal = it }
                if (fatal == null && search(output)) continue
                break
            }
            if (pending.size < 4) break
            val size = SecaProtocol.u16(pending, 2)
            if (pending[0] != 0x55.toByte() || pending[1] != 0xaa.toByte() || size < 11) {
                begin(acquisition, nowNs); continue
            }
            if (pending.size < size) break
            val frame = pending.copyOf(size)
            val info = SecaTypedFrame.inspect(frame)
            val previous = lastCounter
            val advance = if (info != null && previous != null) (info.first - previous) and SecaTypedFrame.MASK else 1L
            if (info == null || (acquisition && info.groups > 0 && advance !in 1..0x7fff_ffffL)) {
                begin(acquisition, nowNs); continue
            }
            output += Piece(frame, true)
            if (acquisition && info.groups > 0) accept(info)
            pending = pending.copyOfRange(size, pending.size)
        }
        if (fatal != null) emitRaw(output, pending.size)
        return Result(output, fatal)
    }

    private fun begin(acquisition: Boolean, nowNs: Long) {
        if (!acquisition || lastCounter == null) {
            fatal = "seca framing recovery forbidden before confirmed ECG; raw bytes retained"
        } else {
            started = nowNs; episodes++; scan = 0; examined = 0; validatedBytes = 0; candidates.clear()
        }
        // Failed setup must not loop back through the same invalid prefix.
        if (fatal != null) started = nowNs
    }

    private fun fail(budget: String) { fatal = "seca recovery exceeded $budget; raw bytes retained" }
    private fun work(): Boolean {
        readWork++
        if (readWork > 32) fail("32 candidate operations per read")
        return fatal == null
    }
    private fun search(output: MutableList<Piece>): Boolean {
        // Incomplete large candidates never block scanning later available offsets.
        for (candidate in candidates.values.toList()) {
            if (!candidate.checked && candidate.offset + candidate.size <= pending.size) {
                validate(candidate)
                if (fatal != null) return false
                if (confirm(output)) return true
            }
        }
        while (scan + 4 <= pending.size) {
            val at = scan++
            if (pending[at] != 0x55.toByte() || pending[at + 1] != 0xaa.toByte()) continue
            if (!work()) return false
            if (++examined > 256) { fail("256 candidate headers"); return false }
            val size = SecaProtocol.u16(pending, at + 2)
            if (size < 11) continue
            val candidate = Candidate(at, size)
            candidates[at] = candidate
            if (at + size <= pending.size) {
                validate(candidate)
                if (fatal != null) return false
                if (confirm(output)) return true
            }
        }
        return false
    }
    private fun validate(candidate: Candidate) {
        if (!work()) return
        validatedBytes += candidate.size
        if (validatedBytes > 262144) { fail("256 KiB validation work"); return }
        candidate.checked = true
        val info = SecaTypedFrame.inspect(pending.copyOfRange(candidate.offset, candidate.offset + candidate.size)) ?: return
        val previous = lastCounter ?: return
        val advance = (info.first - previous) and SecaTypedFrame.MASK
        val lastAdvance = (info.last - previous) and SecaTypedFrame.MASK
        if (info.groups > 0 && advance in 1..maxForward && lastAdvance in 1..maxForward) candidate.info = info
    }
    private fun confirm(output: MutableList<Piece>): Boolean {
        for (first in candidates.values) {
            val a = first.info ?: continue
            val second = candidates[first.offset + first.size] ?: continue
            val b = second.info ?: continue
            if (((b.first - a.last) and SecaTypedFrame.MASK) != 1L) continue
            val firstBytes = pending.copyOfRange(first.offset, first.offset + first.size)
            val secondBytes = pending.copyOfRange(second.offset, second.offset + second.size)
            val consumed = second.offset + second.size
            if (first.offset > 0) {
                output += Piece(pending.copyOf(first.offset), false); corruptBytes += first.offset
            }
            output += Piece(firstBytes, true); output += Piece(secondBytes, true)
            accept(a); accept(b); recovered++
            pending = pending.copyOfRange(consumed, pending.size)
            started = null; candidates.clear()
            return true
        }
        return false
    }
    private fun accept(info: SecaTypedFrame.Info) {
        lastCounter?.let { missingGroups += ((info.first - it) and SecaTypedFrame.MASK) - 1 }
        lastCounter = info.last
    }
    private fun emitRaw(output: MutableList<Piece>, size: Int) {
        if (size > 0) { output += Piece(pending.copyOf(size), false); corruptBytes += size }
        pending = pending.copyOfRange(size, pending.size)
    }
    /** Called once at reader shutdown; unconfirmed candidates never become waveform. */
    fun finish(): List<Piece> {
        val output = mutableListOf<Piece>()
        if (pending.isNotEmpty()) {
            val accepted = started == null && fatal == null
            if (!accepted) corruptBytes += pending.size
            output += Piece(pending, accepted); pending = ByteArray(0)
        }
        return output
    }
}
