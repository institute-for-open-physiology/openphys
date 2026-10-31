package com.vineyardfinance.sensors.location

import android.annotation.SuppressLint
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.location.Location
import android.location.LocationManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.util.Log
import androidx.core.content.ContextCompat
import androidx.core.location.GnssStatusCompat
import androidx.core.location.LocationListenerCompat
import androidx.core.location.LocationManagerCompat
import androidx.core.location.LocationRequestCompat
import com.vineyardfinance.sensors.AppSettings
import com.vineyardfinance.sensors.collector.BoundedIngress
import com.vineyardfinance.sensors.collector.EnqueueResult
import com.vineyardfinance.sensors.collector.RawRecordWriter
import com.vineyardfinance.sensors.data.RecordJson
import android.content.Context
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.sqrt
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Android shell for consent-gated raw location collection
 * (docs/gps_tracking_proposal_20260808.md). Registers GPS + fused location
 * listeners, a GNSS status callback and barometer/accelerometer sensors, and
 * appends whatever the pure [LocationRecorder] returns — nothing else. The
 * owning ECG service only runs this while the per-installation
 * location opt-in is on; this class additionally stops itself as soon as the
 * opt-in turns off.
 */
@SuppressLint("MissingPermission")
internal class LocationCollector(
    private val context: Context,
    private val settings: AppSettings,
    private val writer: RawRecordWriter,
    private val active: () -> Boolean = { settings.collectionEnabled },
    private val onFixStored: (String) -> Unit = {},
) {
    private sealed interface Signal {
        data class Fix(val elapsedMs: Long, val sample: FixSample, val unixNs: Long, val elapsedNs: Long) : Signal
        data class Gnss(val elapsedMs: Long, val sample: GnssStatusSample, val unixNs: Long, val elapsedNs: Long) : Signal
        data class Pressure(val elapsedMs: Long, val sample: PressureSample, val unixNs: Long, val elapsedNs: Long) : Signal
        data class Accel(val elapsedMs: Long, val magnitudeMs2: Double) : Signal
    }

    private val ingress = BoundedIngress<Signal>(SIGNAL_CAPACITY)
    private val droppedSignals = AtomicLong()

    suspend fun run() {
        val locationManager = context.getSystemService(LocationManager::class.java)
            ?: throw IOException("location manager unavailable")
        val sensorManager = context.getSystemService(SensorManager::class.java)
        val pressureSensor = sensorManager?.getDefaultSensor(Sensor.TYPE_PRESSURE)
        val accelSensor = sensorManager?.getDefaultSensor(Sensor.TYPE_ACCELEROMETER)
        val providers = buildList {
            if (LocationManager.GPS_PROVIDER in locationManager.allProviders) add(LocationManager.GPS_PROVIDER)
            if (FUSED_PROVIDER in locationManager.allProviders) add(FUSED_PROVIDER)
        }
        val startElapsedMs = SystemClock.elapsedRealtime()
        val policy = LocationSamplingPolicy(startElapsedMs)
        val recorder = LocationRecorder({ settings.locationCollectionEnabled }, policy, startElapsedMs)
        writer.event(
            "location_collection_started", "info", "raw GPS/location collection active",
            RecordJson.fields(
                mapOf(
                    "providers" to providers.joinToString(","),
                    "has_barometer" to (pressureSensor != null),
                    "has_accelerometer" to (accelSensor != null),
                    "sampling_mode" to policy.mode.wireName,
                    "location_interval_ms" to policy.mode.locationIntervalMs,
                ),
            ),
        )
        if (providers.isEmpty()) {
            writer.event("location_providers_unavailable", "error", "no GPS or fused location provider on this phone")
            throw IOException("no usable location provider")
        }
        val mainExecutor = ContextCompat.getMainExecutor(context)
        val mainHandler = Handler(Looper.getMainLooper())
        val listeners = providers.associateWith { provider ->
            object : LocationListenerCompat {
                override fun onLocationChanged(location: Location) = offer(fixSignal(provider, location))
            }
        }
        val gnssCallback = object : GnssStatusCompat.Callback() {
            override fun onSatelliteStatusChanged(status: GnssStatusCompat) = offer(gnssSignal(status))
        }
        val sensorListener = object : SensorEventListener {
            override fun onSensorChanged(event: SensorEvent) = offer(sensorSignal(event))
            override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) = Unit
        }

        fun requestLocationUpdates(mode: LocationSamplingPolicy.Mode) {
            listeners.forEach { (provider, listener) ->
                LocationManagerCompat.removeUpdates(locationManager, listener)
                val request = LocationRequestCompat.Builder(mode.locationIntervalMs)
                    .setQuality(
                        if (mode == LocationSamplingPolicy.Mode.MOVING) {
                            LocationRequestCompat.QUALITY_HIGH_ACCURACY
                        } else {
                            LocationRequestCompat.QUALITY_LOW_POWER
                        },
                    )
                    .build()
                LocationManagerCompat.requestLocationUpdates(locationManager, provider, request, mainExecutor, listener)
            }
        }

        try {
            requestLocationUpdates(policy.mode)
            if (!LocationManagerCompat.registerGnssStatusCallback(locationManager, mainExecutor, gnssCallback)) {
                writer.event("gnss_status_unavailable", "warn", "GNSS status callback registration was rejected")
            }
            pressureSensor?.let {
                sensorManager.registerListener(sensorListener, it, PRESSURE_SAMPLING_US, SENSOR_BATCH_LATENCY_US, mainHandler)
            }
            accelSensor?.let {
                sensorManager.registerListener(sensorListener, it, ACCEL_SAMPLING_US, SENSOR_BATCH_LATENCY_US, mainHandler)
            }

            while (currentCoroutineContext().isActive && active() && settings.locationCollectionEnabled) {
                // Bounded receive: the timeout doubles as the consent/stop gate
                // poll, so a toggled-off opt-in stops collection within one poll.
                val signal = withTimeoutOrNull(GATE_POLL_MS) { ingress.receive() }
                reportDroppedSignals()
                val transition = when (signal) {
                    is Signal.Fix -> {
                        val output = recorder.onFix(signal.elapsedMs, signal.sample)
                        append(output, signal.unixNs, signal.elapsedNs)
                        if (output.kind != null) onFixStored(signal.sample.provider)
                        output.transition
                    }
                    is Signal.Gnss -> {
                        append(recorder.onGnssStatus(signal.elapsedMs, signal.sample), signal.unixNs, signal.elapsedNs)
                        null
                    }
                    is Signal.Pressure -> {
                        append(recorder.onPressure(signal.elapsedMs, signal.sample), signal.unixNs, signal.elapsedNs)
                        null
                    }
                    is Signal.Accel -> recorder.onAccel(signal.elapsedMs, signal.magnitudeMs2)
                    null -> recorder.onClockTick(SystemClock.elapsedRealtime())
                }
                transition?.let {
                    writer.event(
                        "location_sampling_mode_changed", "info", "${it.from.wireName} -> ${it.to.wireName}",
                        RecordJson.fields(
                            mapOf(
                                "from" to it.from.wireName,
                                "to" to it.to.wireName,
                                "reason" to it.reason,
                                "location_interval_ms" to it.to.locationIntervalMs,
                            ),
                        ),
                    )
                    requestLocationUpdates(it.to)
                }
                recorder.fixWatchdog(SystemClock.elapsedRealtime())?.let { detail ->
                    writer.event("location_fix_stalled", "warn", detail)
                }
            }
        } finally {
            withContext(NonCancellable) {
                var cleanupFailure: String? = null
                runCatching {
                    listeners.values.forEach { LocationManagerCompat.removeUpdates(locationManager, it) }
                }.onFailure { cleanupFailure = "location listener cleanup failed: ${safe(it)}" }
                runCatching {
                    LocationManagerCompat.unregisterGnssStatusCallback(locationManager, gnssCallback)
                }.onFailure { cleanupFailure = cleanupFailure ?: "GNSS callback cleanup failed: ${safe(it)}" }
                runCatching { sensorManager?.unregisterListener(sensorListener) }
                    .onFailure { cleanupFailure = cleanupFailure ?: "sensor listener cleanup failed: ${safe(it)}" }
                ingress.close()
                runCatching {
                    withTimeout(STOP_EVENT_TIMEOUT_MS) {
                        writer.event(
                            "location_collection_stopped",
                            if (cleanupFailure == null) "info" else "error",
                            cleanupFailure ?: "location collection ended",
                        )
                    }
                }.onFailure { Log.e(TAG, "location stop event failed", it) }
            }
        }
    }

    private fun offer(signal: Signal) {
        when (ingress.tryEnqueue(signal)) {
            EnqueueResult.ENQUEUED -> Unit
            EnqueueResult.FULL -> droppedSignals.incrementAndGet()
            // CLOSED only happens during shutdown; the stop event covers it.
            EnqueueResult.CLOSED -> Unit
        }
    }

    private suspend fun reportDroppedSignals() {
        val dropped = droppedSignals.getAndSet(0)
        if (dropped > 0) {
            writer.event(
                "location_signal_dropped", "warn", "location signal buffer overflowed",
                RecordJson.fields(mapOf("dropped_signals" to dropped)),
            )
        }
    }

    private suspend fun append(output: LocationRecorder.Output, unixNs: Long, elapsedNs: Long) {
        val kind = output.kind ?: return
        val body = output.body ?: return
        writer.appendCaptured(kind, body, unixNs, elapsedNs)
    }

    private fun fixSignal(requestedProvider: String, location: Location): Signal.Fix {
        val (unixNs, elapsedNs) = RawRecordWriter.capturedTimes()
        return Signal.Fix(
            elapsedMs = elapsedNs / 1_000_000L,
            sample = FixSample(
                provider = location.provider ?: requestedProvider,
                latitudeDeg = location.latitude,
                longitudeDeg = location.longitude,
                altitudeM = location.altitude.takeIf { location.hasAltitude() },
                horizontalAccuracyM = location.accuracy.toDouble().takeIf { location.hasAccuracy() },
                verticalAccuracyM = location.verticalAccuracyMeters.toDouble()
                    .takeIf { location.hasVerticalAccuracy() },
                speedMps = location.speed.toDouble().takeIf { location.hasSpeed() },
                speedAccuracyMps = location.speedAccuracyMetersPerSecond.toDouble()
                    .takeIf { location.hasSpeedAccuracy() },
                bearingDeg = location.bearing.toDouble().takeIf { location.hasBearing() },
                bearingAccuracyDeg = location.bearingAccuracyDegrees.toDouble()
                    .takeIf { location.hasBearingAccuracy() },
                deviceTimeUnixMs = location.time,
                deviceElapsedRealtimeNs = location.elapsedRealtimeNanos,
                isMock = if (Build.VERSION.SDK_INT >= 31) {
                    location.isMock
                } else {
                    @Suppress("DEPRECATION")
                    location.isFromMockProvider
                },
            ),
            unixNs = unixNs,
            elapsedNs = elapsedNs,
        )
    }

    private fun gnssSignal(status: GnssStatusCompat): Signal.Gnss {
        val (unixNs, elapsedNs) = RawRecordWriter.capturedTimes()
        val usedCn0 = ArrayList<Double>()
        for (index in 0 until status.satelliteCount) {
            if (status.usedInFix(index)) usedCn0.add(status.getCn0DbHz(index).toDouble())
        }
        return Signal.Gnss(
            elapsedMs = elapsedNs / 1_000_000L,
            sample = GnssStatusSample(status.satelliteCount, usedCn0),
            unixNs = unixNs,
            elapsedNs = elapsedNs,
        )
    }

    private fun sensorSignal(event: SensorEvent): Signal = when (event.sensor.type) {
        Sensor.TYPE_PRESSURE -> {
            val (unixNs, elapsedNs) = RawRecordWriter.capturedTimes()
            Signal.Pressure(
                elapsedMs = elapsedNs / 1_000_000L,
                sample = PressureSample(
                    pressureHpa = event.values[0].toDouble(),
                    sensorAccuracy = event.accuracy,
                    sensorTimestampNs = event.timestamp,
                ),
                unixNs = unixNs,
                elapsedNs = elapsedNs,
            )
        }
        else -> {
            val x = event.values.getOrElse(0) { 0f }.toDouble()
            val y = event.values.getOrElse(1) { 0f }.toDouble()
            val z = event.values.getOrElse(2) { 0f }.toDouble()
            Signal.Accel(SystemClock.elapsedRealtime(), sqrt(x * x + y * y + z * z))
        }
    }

    private fun safe(error: Throwable): String = (error.message ?: error.javaClass.simpleName).take(240)

    companion object {
        // LocationManager.FUSED_PROVIDER requires API 31; the literal is stable.
        const val FUSED_PROVIDER = "fused"
        const val SIGNAL_CAPACITY = 512
        const val GATE_POLL_MS = 2_000L
        const val ACCEL_SAMPLING_US = 100_000
        const val PRESSURE_SAMPLING_US = 500_000
        const val SENSOR_BATCH_LATENCY_US = 1_000_000
        const val STOP_EVENT_TIMEOUT_MS = 3_000L
        private const val TAG = "LocationCollector"
    }
}
