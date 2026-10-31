package com.vineyardfinance.sensors

import android.content.Intent

internal sealed interface ConsumedEnrollmentLink {
    data object Absent : ConsumedEnrollmentLink
    data object Invalid : ConsumedEnrollmentLink

    class Valid internal constructor(val token: String) : ConsumedEnrollmentLink {
        override fun toString(): String = "Valid(token=redacted)"
    }
}

/** Removes the fragment-bearing URI from Android-owned intent state before it can be rendered. */
internal object EnrollmentIntent {
    fun consume(source: Intent?): ConsumedEnrollmentLink {
        val rawUri = source?.dataString
        if (source != null) source.data = null
        if (rawUri == null) return ConsumedEnrollmentLink.Absent
        val token = AppLinks.enrollmentToken(rawUri) ?: return ConsumedEnrollmentLink.Invalid
        return ConsumedEnrollmentLink.Valid(token)
    }
}
