package com.vineyardfinance.sensors.collector

/**
 * Applies explicit error-latch semantics to a user-visible collector state.
 *
 * Progress, waiting, and guidance transitions omit [error] and therefore clear
 * stale failure text. Error-bearing transitions must pass the new error they
 * intentionally expose.
 */
internal fun CollectorSnapshot.transitionTo(
    state: String,
    detail: String,
    connected: Boolean = false,
    error: String? = null,
): CollectorSnapshot = copy(
    state = state,
    detail = detail,
    connected = connected,
    lastError = error,
)
