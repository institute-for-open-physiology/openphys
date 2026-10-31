package com.vineyardfinance.sensors.collector

internal enum class CollectorServiceDirective {
    START_RUNTIME,
    HOLD_FAILED_FOREGROUND,
    STOP_SERVICE,
}

internal data class CollectorRuntimeDecision(
    val directive: CollectorServiceDirective,
    val scheduleResidualUpload: Boolean,
) {
    val startRuntime: Boolean get() = directive == CollectorServiceDirective.START_RUNTIME
    val keepForeground: Boolean get() = directive == CollectorServiceDirective.HOLD_FAILED_FOREGROUND
    val stopService: Boolean get() = directive == CollectorServiceDirective.STOP_SERVICE
}

internal data class CollectorCleanupResolution(
    val failureDetail: String?,
    val fatalCleanupFailure: String?,
)

internal object CollectorRuntimePolicy {
    fun onStartCommand(failureLatched: Boolean): CollectorRuntimeDecision = if (failureLatched) {
        holdFailure()
    } else {
        CollectorRuntimeDecision(CollectorServiceDirective.START_RUNTIME, scheduleResidualUpload = false)
    }

    fun afterFatalFailure(collectionDesired: Boolean): CollectorRuntimeDecision = if (collectionDesired) {
        holdFailure()
    } else {
        CollectorRuntimeDecision(CollectorServiceDirective.STOP_SERVICE, scheduleResidualUpload = false)
    }

    fun afterCleanStop(): CollectorRuntimeDecision =
        CollectorRuntimeDecision(CollectorServiceDirective.STOP_SERVICE, scheduleResidualUpload = true)

    fun onUserStop(): CollectorRuntimeDecision =
        CollectorRuntimeDecision(CollectorServiceDirective.STOP_SERVICE, scheduleResidualUpload = false)

    fun resolveCleanup(
        primaryFailure: String?,
        cleanupFailure: String?,
        collectionDesired: Boolean,
    ): CollectorCleanupResolution = CollectorCleanupResolution(
        failureDetail = primaryFailure ?: cleanupFailure,
        fatalCleanupFailure = cleanupFailure?.takeIf {
            primaryFailure == null && collectionDesired
        },
    )

    fun failedStatus(current: CollectorSnapshot, failureDetail: String): CollectorSnapshot = current.copy(
        state = "Collection failed",
        detail = "Action required: stop, then start collection. ${failureDetail.take(MAX_FAILURE_DETAIL_LENGTH)}",
        serviceRunning = true,
        connected = false,
        lastError = failureDetail.take(MAX_FAILURE_DETAIL_LENGTH),
    )

    private fun holdFailure() = CollectorRuntimeDecision(
        directive = CollectorServiceDirective.HOLD_FAILED_FOREGROUND,
        scheduleResidualUpload = false,
    )

    private const val MAX_FAILURE_DETAIL_LENGTH = 240
}
