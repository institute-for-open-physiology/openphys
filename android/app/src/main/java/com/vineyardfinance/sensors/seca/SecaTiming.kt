package com.vineyardfinance.sensors.seca

import android.util.Log

/** Diagnostic timing only; never includes bytes, credentials or configuration values. */
object SecaTiming {
    private data class Sample(var count: Int = 0, var totalNs: Long = 0, var maxNs: Long = 0)
    private val samples = linkedMapOf<String, Sample>()
    private var since = System.nanoTime()
    @Synchronized fun record(operation: String, elapsedNs: Long) {
        val sample = samples.getOrPut(operation) { Sample() }
        sample.count++; sample.totalNs += elapsedNs; sample.maxNs = maxOf(sample.maxNs, elapsedNs)
        val now = System.nanoTime()
        if (now - since >= 1_000_000_000L) {
            if (samples.values.any { it.maxNs >= 50_000_000L }) {
                Log.i("SecaTiming", samples.entries.joinToString("; ") { (name, value) ->
                    "$name n=${value.count} avg_us=${value.totalNs / value.count / 1000} max_us=${value.maxNs / 1000}"
                })
            }
            samples.clear(); since = now
        }
    }
    fun <T> measure(operation: String, action: () -> T): T {
        val start = System.nanoTime()
        try { return action() } finally { record(operation, System.nanoTime() - start) }
    }
}
