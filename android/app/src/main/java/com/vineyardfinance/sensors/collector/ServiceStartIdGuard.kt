package com.vineyardfinance.sensors.collector

/**
 * Stops a started service only against the latest command delivered to it.
 *
 * Android compares this identifier with its own latest start request, including
 * a request which ActivityManager has accepted but this service has not yet
 * observed. That makes [stopIfLatest] safe at asynchronous runtime boundaries.
 */
internal class ServiceStartIdGuard(
    private val stopSelfResult: (Int) -> Boolean,
) {
    private var latestDeliveredStartId: Int? = null

    fun commandDelivered(startId: Int) {
        latestDeliveredStartId = startId
    }

    fun stopIfLatest(): Boolean = latestDeliveredStartId?.let(stopSelfResult) ?: false
}
