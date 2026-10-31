package com.vineyardfinance.sensors.location

import java.util.Locale
import kotlin.math.sqrt

/**
 * Motion-adaptive GPS sampling (docs/gps_tracking_proposal_20260808.md):
 * full-rate fixes while the wearer is moving — exactly when Doppler speed
 * matters — and occasional low-power fixes while stationary so the phone
 * still lasts a full collection day.
 *
 * Pure logic. The Android shell feeds accelerometer magnitudes and Doppler
 * speeds, and re-registers platform location listeners whenever a
 * [Transition] is returned. Every transition is also recorded as a
 * collector event so sampling gaps stay explainable.
 */
class LocationSamplingPolicy(startElapsedMs: Long) {

    enum class Mode(
        val locationIntervalMs: Long,
        val pressureIntervalMs: Long,
        /** No fix for this long means the fix stream has stalled. */
        val fixStallAfterMs: Long,
        val wireName: String,
    ) {
        MOVING(1_000L, 1_000L, 30_000L, "moving"),
        STATIONARY(180_000L, 60_000L, 360_000L, "stationary"),
    }

    data class Transition(val from: Mode, val to: Mode, val reason: String)

    /**
     * Starting in MOVING is the safe default for data quality: a run begun
     * right after enabling gets 1 Hz immediately, and a genuinely still
     * phone settles to STATIONARY after the stillness hold.
     */
    var mode: Mode = Mode.MOVING
        private set

    private var lastMotionEvidenceElapsedMs: Long = startElapsedMs
    private var windowStartElapsedMs: Long = startElapsedMs
    private val window = ArrayList<Double>()

    /** Accelerometer magnitude sample; returns a transition when the mode changes. */
    fun onAccelSample(elapsedMs: Long, magnitudeMs2: Double): Transition? {
        window.add(magnitudeMs2)
        if (elapsedMs - windowStartElapsedMs < ACCEL_WINDOW_MS) return null
        val deviation = standardDeviation(window)
        window.clear()
        windowStartElapsedMs = elapsedMs
        return if (deviation >= ACCEL_STDDEV_MOVING_MS2) {
            motionEvidence(
                elapsedMs,
                String.format(Locale.ROOT, "accelerometer stddev %.2f m/s^2", deviation),
            )
        } else {
            stillnessCheck(elapsedMs)
        }
    }

    /** Doppler speed from a location fix corroborates motion. */
    fun onSpeedSample(elapsedMs: Long, speedMps: Double): Transition? =
        if (speedMps >= SPEED_MOVING_MPS) {
            motionEvidence(elapsedMs, String.format(Locale.ROOT, "doppler speed %.1f m/s", speedMps))
        } else {
            stillnessCheck(elapsedMs)
        }

    /** Called on collector gate polls so a signal-free phone can still settle. */
    fun onClockTick(elapsedMs: Long): Transition? = stillnessCheck(elapsedMs)

    private fun motionEvidence(elapsedMs: Long, reason: String): Transition? {
        lastMotionEvidenceElapsedMs = elapsedMs
        if (mode == Mode.MOVING) return null
        val transition = Transition(mode, Mode.MOVING, reason)
        mode = Mode.MOVING
        return transition
    }

    private fun stillnessCheck(elapsedMs: Long): Transition? {
        if (mode == Mode.STATIONARY) return null
        if (elapsedMs - lastMotionEvidenceElapsedMs < STILL_HOLD_MS) return null
        val transition = Transition(mode, Mode.STATIONARY, "no motion evidence for ${STILL_HOLD_MS / 1_000} s")
        mode = Mode.STATIONARY
        return transition
    }

    companion object {
        const val ACCEL_WINDOW_MS = 5_000L
        const val ACCEL_STDDEV_MOVING_MS2 = 0.5
        const val SPEED_MOVING_MPS = 1.0
        const val STILL_HOLD_MS = 120_000L

        internal fun standardDeviation(values: List<Double>): Double {
            if (values.size < 2) return 0.0
            val mean = values.sum() / values.size
            val variance = values.sumOf { (it - mean) * (it - mean) } / values.size
            return sqrt(variance)
        }
    }
}
