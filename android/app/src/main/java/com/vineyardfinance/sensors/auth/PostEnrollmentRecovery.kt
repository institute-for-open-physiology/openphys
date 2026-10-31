package com.vineyardfinance.sensors.auth

/** Restarts durable delivery after a replacement credential without requiring a Stop→Start cycle. */
internal class PostEnrollmentRecovery(
    private val collectionDesired: () -> Boolean,
    private val setupReady: () -> Boolean,
    private val enqueueResidualUpload: () -> Unit,
    private val startCollector: () -> Unit,
    private val reportFailure: (String) -> Unit,
) {
    fun resume() {
        runAction("Buffered upload resume", enqueueResidualUpload)
        val shouldStart = runCatching { collectionDesired() && setupReady() }.getOrElse {
            reportFailure("Collection resume check failed: ${safe(it)}")
            false
        }
        if (shouldStart) runAction("Collector resume", startCollector)
    }

    private fun runAction(label: String, action: () -> Unit) {
        runCatching(action).onFailure { reportFailure("$label failed: ${safe(it)}") }
    }

    private fun safe(error: Throwable): String =
        (error.message ?: error.javaClass.simpleName).take(160)
}
