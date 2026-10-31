package com.vineyardfinance.sensors.network

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Keeps the upload loop running while collection is enabled: restarts it whenever
 * [UploadStallPolicy] finds it silently stalled, it dies abnormally, or it exits while
 * collection is still enabled. The recovery path itself must never hang, so every await it
 * performs is bounded: the backlog read and both callbacks get [telemetryTimeoutMs], and the
 * post-cancel join gets [joinTimeoutMs] — a loop that stays blocked past that (a stuck await
 * cancellation cannot break) is abandoned and a fresh loop starts beside it. Abandonment is
 * data-safe: batches are durable and idempotent, and concurrent drains are already tolerated.
 * An abandoned coroutine only delays the enclosing job's completion, which nothing awaits.
 * Callbacks are best-effort; recovery never rides on telemetry.
 */
class UploadLoopSupervisor(
    private val vitals: UploadLoopVitals,
    private val queuedRecords: suspend () -> Long,
    private val onStall: suspend (UploadStallPolicy.Report) -> Unit,
    private val onLoopFailure: suspend (String) -> Unit = {},
    private val clock: () -> Long = System::currentTimeMillis,
    private val checkIntervalMs: Long = CHECK_INTERVAL_MS,
    private val joinTimeoutMs: Long = JOIN_TIMEOUT_MS,
    private val telemetryTimeoutMs: Long = TELEMETRY_TIMEOUT_MS,
) {
    suspend fun supervise(active: () -> Boolean, runLoop: suspend () -> Unit): Unit = coroutineScope {
        while (active()) {
            var abnormalExit: String? = null
            val loop = launch {
                try {
                    runLoop()
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Exception) {
                    abnormalExit = "upload loop died: ${(error.message ?: error.javaClass.simpleName).take(160)}"
                }
            }
            var stalled = false
            while (loop.isActive) {
                delay(checkIntervalMs)
                if (!active()) {
                    // Collection stopped; a healthy loop exits on its own, a stuck one only
                    // through cancellation. Batches are durable and idempotent, so
                    // cancelling mid-upload is always safe.
                    loop.cancel()
                    break
                }
                if (!loop.isActive) break
                val report = UploadStallPolicy.evaluate(clock(), vitals.snapshot(), boundedQueuedRecords())
                if (report != null) {
                    stalled = true
                    // Break the stuck await first; the report must never delay recovery.
                    loop.cancel()
                    bestEffort { onStall(report) }
                    break
                }
            }
            val joined = withTimeoutOrNull(joinTimeoutMs) { loop.join() } != null
            val exitDetail = abnormalExit
            when {
                !joined -> bestEffort {
                    onLoopFailure(
                        "upload loop still blocked $joinTimeoutMs ms after cancellation; " +
                            "abandoning it and starting a fresh loop",
                    )
                }
                exitDetail != null -> bestEffort { onLoopFailure(exitDetail) }
            }
            if (joined && !stalled && exitDetail == null) {
                if (!active()) break
                // The loop's contract is to return only when collection is disabled; a
                // normal return with collection enabled would otherwise end supervision
                // silently — the exact death class this watchdog exists to prevent.
                bestEffort { onLoopFailure("upload loop exited while collection remained enabled; relaunching") }
                delay(checkIntervalMs)
                continue
            }
            // An instantly dying loop must not relaunch hot; a stall already waited 5 min.
            if (joined && exitDetail != null) delay(checkIntervalMs)
        }
    }

    private suspend fun boundedQueuedRecords(): Long = try {
        withTimeout(telemetryTimeoutMs) { queuedRecords() }
    } catch (timeout: TimeoutCancellationException) {
        -1L
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        -1L
    }

    private suspend fun bestEffort(block: suspend () -> Unit) {
        try {
            withTimeout(telemetryTimeoutMs) { block() }
        } catch (timeout: TimeoutCancellationException) {
            // Telemetry must never block loop recovery.
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // Telemetry must never block loop recovery.
        }
    }

    companion object {
        const val CHECK_INTERVAL_MS = 30_000L
        const val JOIN_TIMEOUT_MS = 10_000L
        const val TELEMETRY_TIMEOUT_MS = 10_000L
    }
}
