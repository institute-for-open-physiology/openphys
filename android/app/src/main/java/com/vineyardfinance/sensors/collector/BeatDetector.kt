package com.vineyardfinance.sensors.collector

import kotlin.math.*

/** Same causal R-peak detector as H10Kit; never interprets heart health. */
class BeatDetector(rate: Double = 130.0) {
    private val window = max(1, (rate * .15).toInt())
    private val refractory = (rate * .25).toInt()
    private val warmup = (rate * 2).toInt()
    private val longest = (rate * .3).toInt()
    private val decay = .5.pow(1 / (rate * 3))
    private val previous = ArrayDeque<Float>()
    private val energy = DoubleArray(window)
    private var slot = 0
    private var sum = 0.0
    private var level = 0.0
    private var baseline: Float? = null
    private val recent = ArrayDeque<Pair<Int, Float>>()
    private var beatStart: Int? = null
    private var best = 0 to 0f
    private var lastBeat = Int.MIN_VALUE / 2
    private var seen = 0

    fun push(value: Float, index: Int): Int? {
        seen++
        val base = baseline?.let { it + (value - it) * .02f } ?: value
        baseline = base
        val height = abs(value - base)
        recent.addLast(index to height)
        if (recent.size > window) recent.removeFirst()
        val slope = if (previous.size == 2) (value - previous.first()).toDouble() else 0.0
        previous.addLast(value)
        if (previous.size > 2) previous.removeFirst()
        sum += slope * slope - energy[slot]
        energy[slot] = slope * slope
        slot = (slot + 1) % window
        val integrated = max(0.0, sum) / window
        level = max(integrated, level * decay)
        val above = seen > warmup && integrated > level * .3
        val start = beatStart
        if (start == null) {
            if (above && index - lastBeat > refractory) {
                beatStart = index
                best = recent.maxBy { it.second }
            }
            return null
        }
        if (height > best.second) best = index to height
        if (above && index - start < longest) return null
        beatStart = null
        lastBeat = best.first
        return best.first
    }
}
