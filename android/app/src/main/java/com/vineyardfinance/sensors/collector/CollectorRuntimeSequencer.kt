package com.vineyardfinance.sensors.collector

/**
 * Serializes ownership of one collector runtime token on the service thread.
 *
 * A start requested while the current token is cancelling is remembered but
 * cannot launch until that exact token completes. Completion from any stale
 * token is ignored, so old cleanup can never authorize teardown of a newer run.
 * All methods are called from Android's main service thread in production.
 */
internal class CollectorRuntimeSequencer<T : Any>(
    private val isActive: (T) -> Boolean,
    private val isCompleted: (T) -> Boolean,
    private val cancel: (T) -> Unit,
) {
    enum class StartDecision { STARTED, ALREADY_RUNNING, DEFERRED, CLOSED }
    enum class StopDecision { STOP_NOW, WAIT_FOR_COMPLETION }
    enum class CompletionDecision { IGNORE, STOP_SERVICE, RESTART }

    private var current: T? = null
    private var restartRequested = false
    private var closed = false

    fun hasActiveRuntime(): Boolean = current?.let(isActive) == true

    fun requestStart(launch: () -> T): StartDecision {
        if (closed) return StartDecision.CLOSED
        val token = current
        return when {
            token == null || isCompleted(token) -> {
                restartRequested = false
                current = launch()
                StartDecision.STARTED
            }
            isActive(token) -> StartDecision.ALREADY_RUNNING
            else -> {
                restartRequested = true
                StartDecision.DEFERRED
            }
        }
    }

    fun requestStop(): StopDecision {
        restartRequested = false
        val token = current
        if (token == null || isCompleted(token)) return StopDecision.STOP_NOW
        cancel(token)
        return StopDecision.WAIT_FOR_COMPLETION
    }

    fun completed(token: T): CompletionDecision {
        if (closed || current !== token) return CompletionDecision.IGNORE
        current = null
        return if (restartRequested) {
            restartRequested = false
            CompletionDecision.RESTART
        } else {
            CompletionDecision.STOP_SERVICE
        }
    }

    fun close() {
        if (closed) return
        closed = true
        restartRequested = false
        current?.takeUnless(isCompleted)?.let(cancel)
        current = null
    }
}
