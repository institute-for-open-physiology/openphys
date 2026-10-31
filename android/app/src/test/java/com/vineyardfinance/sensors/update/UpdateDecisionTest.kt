package com.vineyardfinance.sensors.update

import org.junit.Assert.assertEquals
import org.junit.Test

class UpdateDecisionTest {
    private fun preview(code: Long) = AppUpdateClient.ApkPreview(
        versionCode = code,
        versionName = "0.9.$code",
        sha256 = "ab".repeat(32),
        expiresAtUnixMs = 1_000,
        ticket = "ticket",
    )

    @Test fun notEnrolledAlwaysProceedsWithConnection() {
        assertEquals(
            UpdateDecision.Action.ProceedWithConnection,
            UpdateDecision.forScannedLink(false, 7, preview(99)),
        )
        assertEquals(
            UpdateDecision.Action.ProceedWithConnection,
            UpdateDecision.forScannedLink(false, 7, null),
        )
    }

    @Test fun enrolledWithNewerServerVersionOffersTheUpdate() {
        assertEquals(
            UpdateDecision.Action.UpdateAvailable(8, "0.9.8"),
            UpdateDecision.forScannedLink(true, 7, preview(8)),
        )
    }

    @Test fun enrolledWithSameOrOlderServerVersionIsUpToDate() {
        assertEquals(
            UpdateDecision.Action.AlreadyUpToDate,
            UpdateDecision.forScannedLink(true, 7, preview(7)),
        )
        assertEquals(
            UpdateDecision.Action.AlreadyUpToDate,
            UpdateDecision.forScannedLink(true, 7, preview(6)),
        )
    }

    @Test fun enrolledWithFailedCheckNeverExchangesAndNeverOffersAnUpdate() {
        assertEquals(
            UpdateDecision.Action.CheckUnavailable,
            UpdateDecision.forScannedLink(true, 7, null),
        )
    }
}
