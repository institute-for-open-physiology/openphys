package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.data.HistoryBin
import com.vineyardfinance.sensors.data.SignalStore
import java.util.UUID
import kotlin.math.*

data class SignalPoint(val time: Double, val value: Double?)

data class SignalSnapshot(
    val bpm: Int? = null,
    val contact: Boolean? = null,
    val rr: Double? = null,
    val ecg: List<SignalPoint> = emptyList(),
    val acceleration: List<Double>? = null,
    val gravity: List<Double> = listOf(-1000.0, 0.0, 0.0),
    val moving: Boolean = false,
    val current: List<Double?> = List(5) { null },
    val clockSynced: Boolean = false,
    val heartRate: List<SignalPoint> = emptyList(),
    val error: String? = null,
    val beats: List<Double> = emptyList(),
)

/** Bounded drawing buffers. Access only through synchronized methods. */
class LiveSignals(private val store: SignalStore) {
    private val ecg = ArrayDeque<SignalPoint>()
    private val heartRate = ArrayDeque<SignalPoint>()
    private var hr: SensorDecoder.HeartRate? = null
    private var hrAt = Double.NEGATIVE_INFINITY
    private var accAt = Double.NEGATIVE_INFINITY
    private var ecgNext: Double? = null
    private var detector = BeatDetector()
    private var ecgIndex = 0
    private val beats = ArrayDeque<Double>()
    private var acceleration: List<Double>? = null
    private var gravity = listOf(-1000.0, 0.0, 0.0)
    private var moving = false
    private var clockSynced = false
    private var offsetMinutes = 0
    private var binTime: Double? = null
    private val sums = DoubleArray(5)
    private val counts = IntArray(5)
    private var motionSum = 0.0
    private var motionSquares = 0.0
    private var motionCount = 0
    private var failure: String? = null

    @Synchronized
    fun receive(uuid: UUID, bytes: ByteArray, time: Double) {
        roll(time)
        if (uuid == H10Protocol.HR_MEASUREMENT) {
            hr = SensorDecoder.parseHeartRate(bytes)
            hrAt = time
            hr?.takeIf { it.hasHeartRate }
                ?.let { sample ->
                    add(0, sample.bpm.toDouble())
                    heartRate.addLast(SignalPoint(time, sample.bpm.toDouble()))
                    while (
                        heartRate.size > 2400 ||
                            (heartRate.firstOrNull()?.time ?: time) < time - 600
                    ) heartRate.removeFirst()
                    sample.rrMs.forEach { add(1, it) }
                }
        } else if (uuid == H10Protocol.PMD_DATA) {
            val frame = SensorDecoder.parseFrame(bytes) ?: return
            if (clockSynced)
                add(4, (time - 946684800 + offsetMinutes * 60) * 1000 - frame.timestampNs / 1e6)
            SensorDecoder.decodeECG(frame)
                ?.takeIf { it.isNotEmpty() }
                ?.let { samples ->
                    val ideal = time - (samples.size - 1) / 130.0
                    val continued = ecgNext?.takeIf { abs(it - ideal) < 0.5 }
                    if (continued == null) detector = BeatDetector()
                    val start = continued?.let { it + (ideal - it) * 0.02 } ?: ideal
                    samples.forEachIndexed { i, value ->
                        val sampleTime = start + i / 130.0
                        ecg.addLast(SignalPoint(sampleTime, value.toDouble()))
                        detector.push(value.toFloat(), ecgIndex)?.let { peak ->
                            beats.addLast(sampleTime - (ecgIndex - peak) / 130.0)
                            while (beats.size > 60) beats.removeFirst()
                        }
                        ecgIndex++
                    }
                    ecgNext = start + samples.size / 130.0
                    while (ecg.size > 3900) ecg.removeFirst()
                }
            SensorDecoder.decodeACC(frame)
                ?.takeIf { it.isNotEmpty() }
                ?.let { samples ->
                    if (time - accAt > 2.5) gravity = samples.first().map(Int::toDouble)
                    samples.forEach { row ->
                        gravity = (0..2).map { gravity[it] + 0.005 * (row[it] - gravity[it]) }
                        val magnitude = sqrt(row.sumOf { it.toDouble() * it })
                        motionSum += magnitude
                        motionSquares += magnitude * magnitude
                        motionCount++
                    }
                    acceleration = samples.last().map(Int::toDouble)
                    moving =
                        sqrt((0..2).sumOf { (acceleration!![it] - gravity[it]).pow(2) }) > 120 ||
                            abs(sqrt(gravity.sumOf { it * it }) - 1000) > 150
                    accAt = time
                }
        }
    }

    @Synchronized
    fun receiveRSSI(value: Int, time: Double) {
        roll(time)
        add(3, value.toDouble())
    }

    @Synchronized
    fun setClock(synced: Boolean, offset: Int = 0) {
        clockSynced = synced
        offsetMinutes = offset
    }

    @Synchronized
    fun disconnect() {
        flush()
        hr = null
        hrAt = Double.NEGATIVE_INFINITY
        accAt = Double.NEGATIVE_INFINITY
        clockSynced = false
        ecgNext = null
    }

    @Synchronized
    fun snapshot(time: Double): SignalSnapshot =
        SignalSnapshot(
            hr?.takeIf { time - hrAt in 0.0..<2.5 && it.hasHeartRate }?.bpm,
            hr?.contact(time - hrAt),
            hr?.takeIf { time - hrAt in 0.0..<2.5 && it.hasHeartRate }?.rrMs?.lastOrNull(),
            ecg.toList(),
            acceleration.takeIf { time - accAt in 0.0..<2.5 },
            gravity,
            moving,
            values(),
            clockSynced,
            heartRate.toList(),
            failure,
            beats.toList(),
        )

    @Synchronized fun currentBin(): HistoryBin? = binTime?.let { HistoryBin(it, values()) }

    private fun add(index: Int, value: Double) {
        if (value.isFinite()) {
            sums[index] += value
            counts[index]++
        }
    }

    private fun values(): List<Double?> =
        (0..4).map { index ->
            if (index == 2 && motionCount >= 20)
                sqrt(max(0.0, motionSquares / motionCount - (motionSum / motionCount).pow(2)))
            else if (counts[index] > 0) sums[index] / counts[index] else null
        }

    @Synchronized
    fun flush() {
        val time = binTime ?: return
        try {
            store.saveBin(HistoryBin(time, values()))
            failure = null
        } catch (_: Exception) {
            failure = "History could not be saved."
        }
    }

    private fun roll(time: Double) {
        val next = floor(time / 5) * 5
        val previous = binTime
        if (previous != null && next <= previous) return
        if (previous != null) flush()
        binTime = next
        sums.fill(0.0)
        counts.fill(0)
        motionSum = 0.0
        motionSquares = 0.0
        motionCount = 0
    }
}
