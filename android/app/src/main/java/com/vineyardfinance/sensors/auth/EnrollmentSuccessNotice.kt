package com.vineyardfinance.sensors.auth

internal data class EnrollmentSuccessPresentation(
    val visible: Boolean,
    val focusAndAnnounce: Boolean = false,
    val successToAcknowledge: EnrollmentState.Succeeded? = null,
    val remainingMillis: Long? = null,
)

/**
 * Converts a one-shot exchange result into bounded UI state without treating a stored credential
 * as a new success. Deadlines use elapsed realtime so wall-clock changes cannot prolong the notice.
 */
internal class EnrollmentSuccessNotice(
    private val elapsedRealtimeMillis: () -> Long,
    private val displayDurationMillis: Long = DISPLAY_DURATION_MILLIS,
) {
    private var visibleUntilElapsedRealtimeMillis: Long? = null
    private var restoredNotice = false
    private var observedSuccess: EnrollmentState.Succeeded? = null

    init {
        require(displayDurationMillis > 0L)
    }

    fun restore(savedDeadline: Long?) {
        observedSuccess = null
        val now = elapsedRealtimeMillis()
        visibleUntilElapsedRealtimeMillis = savedDeadline?.takeIf { deadline ->
            remaining(deadline, now) != null
        }
        restoredNotice = visibleUntilElapsedRealtimeMillis != null
    }

    fun present(state: EnrollmentState): EnrollmentSuccessPresentation {
        val now = elapsedRealtimeMillis()
        expireIfNeeded(now)
        return when (state) {
            is EnrollmentState.Succeeded -> presentSuccess(state, now)
            EnrollmentState.Exchanging,
            is EnrollmentState.Failed,
            -> {
                clear()
                HIDDEN
            }
            EnrollmentState.Idle -> current(now)
        }
    }

    fun current(): EnrollmentSuccessPresentation = current(elapsedRealtimeMillis())

    fun savedDeadline(): Long? {
        val now = elapsedRealtimeMillis()
        expireIfNeeded(now)
        return visibleUntilElapsedRealtimeMillis
    }

    private fun presentSuccess(
        success: EnrollmentState.Succeeded,
        now: Long,
    ): EnrollmentSuccessPresentation {
        val firstObservation = observedSuccess !== success
        observedSuccess = success
        val shouldFocusAndAnnounce = firstObservation && !restoredNotice
        if (firstObservation && visibleUntilElapsedRealtimeMillis == null) {
            visibleUntilElapsedRealtimeMillis = now.saturatingAdd(displayDurationMillis)
        }
        restoredNotice = false
        return current(now).copy(
            focusAndAnnounce = shouldFocusAndAnnounce,
            successToAcknowledge = success,
        )
    }

    private fun current(now: Long): EnrollmentSuccessPresentation {
        expireIfNeeded(now)
        val deadline = visibleUntilElapsedRealtimeMillis ?: return HIDDEN
        return EnrollmentSuccessPresentation(
            visible = true,
            remainingMillis = requireNotNull(remaining(deadline, now)),
        )
    }

    private fun expireIfNeeded(now: Long) {
        val deadline = visibleUntilElapsedRealtimeMillis ?: return
        if (remaining(deadline, now) == null) clear()
    }

    private fun clear() {
        visibleUntilElapsedRealtimeMillis = null
        restoredNotice = false
    }

    private fun remaining(deadline: Long, now: Long): Long? {
        if (deadline <= now) return null
        val value = deadline - now
        return value.takeIf { it in 1L..displayDurationMillis }
    }

    private fun Long.saturatingAdd(increment: Long): Long =
        if (this > Long.MAX_VALUE - increment) Long.MAX_VALUE else this + increment

    companion object {
        const val DISPLAY_DURATION_MILLIS = 120_000L
        private val HIDDEN = EnrollmentSuccessPresentation(visible = false)
    }
}
