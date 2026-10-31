package com.vineyardfinance.sensors.network

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class UploadWorkPolicyTest {
    @Test fun transientFailureStillRetriesAtAttempt20() {
        assertTransientRetry(20)
    }

    @Test fun transientFailureStillRetriesAtAttempt21() {
        assertTransientRetry(21)
    }

    @Test fun transientFailureStillRetriesAtVeryHighAttemptCount() {
        assertTransientRetry(Int.MAX_VALUE)
    }

    @Test fun stoppedServiceResidualWorkerRetainsItsOnlyGuaranteedRecoveryTrigger() {
        listOf(0, 1, 19, 20, 21, 1_000_000, Int.MAX_VALUE).forEach(::assertTransientRetry)
    }

    @Test fun everyNonterminalOutageClassRetriesWithoutAnAttemptCeiling() {
        listOf(
            "network unavailable",
            "HTTP 408",
            "HTTP 429",
            "HTTP 503",
            "generic transient failure",
        ).forEach { error ->
            assertEquals(
                error,
                UploadWorkOutcome.RETRY,
                UploadWorkPolicy.decide(
                    credentialAvailable = true,
                    drain = drain(complete = false, error = error),
                    runAttemptCount = Int.MAX_VALUE,
                ),
            )
        }
    }

    @Test fun completeDrainSucceedsButReconnectAndMissingCredentialDoNotChurn() {
        assertEquals(
            UploadWorkOutcome.SUCCESS,
            UploadWorkPolicy.decide(true, drain(complete = true), Int.MAX_VALUE),
        )
        assertEquals(
            UploadWorkOutcome.FAILURE,
            UploadWorkPolicy.decide(
                true,
                drain(complete = false, reconnectRequired = true),
                Int.MAX_VALUE,
            ),
        )
        assertEquals(
            UploadWorkOutcome.FAILURE,
            UploadWorkPolicy.decide(false, null, Int.MAX_VALUE),
        )
    }

    @Test fun invalidPolicyInputsFailLoudly() {
        assertTrue(runCatching { UploadWorkPolicy.decide(true, null, 0) }.isFailure)
        assertTrue(runCatching { UploadWorkPolicy.decide(false, null, -1) }.isFailure)
    }

    private fun assertTransientRetry(attempt: Int) {
        assertEquals(
            "attempt=$attempt",
            UploadWorkOutcome.RETRY,
            UploadWorkPolicy.decide(
                credentialAvailable = true,
                drain = drain(complete = false),
                runAttemptCount = attempt,
            ),
        )
    }

    private fun drain(
        complete: Boolean,
        reconnectRequired: Boolean = false,
        error: String = "transient outage",
    ) = IngestUploader.DrainResult(
        uploadedRecords = 0,
        complete = complete,
        error = if (complete) null else error,
        reconnectRequired = reconnectRequired,
    )
}
