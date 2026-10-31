package com.vineyardfinance.sensors.location

import com.google.gson.JsonObject

/**
 * Consent-gated production of location records.
 *
 * Every record-producing path re-checks the per-installation location opt-in
 * at the moment the record would be produced: with the toggle off this class
 * returns nothing, even if platform listeners are somehow still delivering
 * signals. The Android shell only appends what this class returns, so a
 * disabled toggle produces zero location records
 * (docs/agreed_app_changes_20260808.md item 9).
 *
 * Also owns the record-rate policy: GNSS status at low rate, barometer
 * throttled per sampling mode, and a fix-stall watchdog so gaps are loud.
 */
class LocationRecorder(
    private val consentGranted: () -> Boolean,
    private val policy: LocationSamplingPolicy,
    startElapsedMs: Long,
) {
    /** A record to append (both null when nothing may be recorded) plus any mode transition. */
    data class Output(
        val kind: String? = null,
        val body: JsonObject? = null,
        val transition: LocationSamplingPolicy.Transition? = null,
    )

    val mode: LocationSamplingPolicy.Mode get() = policy.mode

    private var lastGnssStatusElapsedMs = startElapsedMs - GNSS_STATUS_INTERVAL_MS
    private var lastPressureElapsedMs = startElapsedMs - policy.mode.pressureIntervalMs
    private var lastFixElapsedMs = startElapsedMs
    private var fixStallReported = false

    fun onFix(elapsedMs: Long, fix: FixSample): Output {
        if (!consentGranted()) return Output()
        lastFixElapsedMs = elapsedMs
        fixStallReported = false
        val transition = fix.speedMps?.let { policy.onSpeedSample(elapsedMs, it) }
        return Output(
            kind = LocationRecords.LOCATION_FIX,
            body = LocationRecords.locationFixBody(fix, policy.mode.wireName),
            transition = transition,
        )
    }

    fun onGnssStatus(elapsedMs: Long, sample: GnssStatusSample): Output {
        if (!consentGranted()) return Output()
        if (elapsedMs - lastGnssStatusElapsedMs < GNSS_STATUS_INTERVAL_MS) return Output()
        lastGnssStatusElapsedMs = elapsedMs
        return Output(LocationRecords.GNSS_STATUS, LocationRecords.gnssStatusBody(sample))
    }

    fun onPressure(elapsedMs: Long, sample: PressureSample): Output {
        if (!consentGranted()) return Output()
        if (elapsedMs - lastPressureElapsedMs < policy.mode.pressureIntervalMs) return Output()
        lastPressureElapsedMs = elapsedMs
        return Output(
            LocationRecords.PRESSURE_SAMPLE,
            LocationRecords.pressureSampleBody(sample, policy.mode.wireName),
        )
    }

    /** Accelerometer input feeds the sampling policy only; it never produces a record. */
    fun onAccel(elapsedMs: Long, magnitudeMs2: Double): LocationSamplingPolicy.Transition? =
        policy.onAccelSample(elapsedMs, magnitudeMs2)

    /** Gate-poll input so a signal-free phone can still settle to low power. */
    fun onClockTick(elapsedMs: Long): LocationSamplingPolicy.Transition? =
        policy.onClockTick(elapsedMs)

    /** Returns a warning detail once per stall when fixes stop arriving. */
    fun fixWatchdog(elapsedMs: Long): String? {
        if (!consentGranted()) return null
        if (fixStallReported || elapsedMs - lastFixElapsedMs < policy.mode.fixStallAfterMs) return null
        fixStallReported = true
        return "no location fix for ${(elapsedMs - lastFixElapsedMs) / 1_000} s in ${policy.mode.wireName} mode"
    }

    companion object {
        const val GNSS_STATUS_INTERVAL_MS = 10_000L
    }
}
