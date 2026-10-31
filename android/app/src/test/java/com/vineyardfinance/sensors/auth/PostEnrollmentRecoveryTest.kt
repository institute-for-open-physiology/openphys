package com.vineyardfinance.sensors.auth

import org.junit.Assert.assertEquals
import org.junit.Test

class PostEnrollmentRecoveryTest {
    @Test fun reEnrollmentAlwaysReschedulesQueueAndRestartsDesiredReadyCollector() {
        val actions = mutableListOf<String>()
        val recovery = recovery(
            desired = true,
            ready = true,
            enqueue = { actions += "enqueue buffered upload" },
            start = { actions += "start collector" },
        )

        recovery.resume()

        assertEquals(listOf("enqueue buffered upload", "start collector"), actions)
    }

    @Test fun bufferedUploadResumesEvenWhenCollectionIsNotDesiredOrSetupIsIncomplete() {
        listOf(false to true, true to false, false to false).forEach { (desired, ready) ->
            val actions = mutableListOf<String>()

            recovery(
                desired = desired,
                ready = ready,
                enqueue = { actions += "enqueue buffered upload" },
                start = { actions += "start collector" },
            ).resume()

            assertEquals(listOf("enqueue buffered upload"), actions)
        }
    }

    @Test fun enqueueFailureDoesNotPreventDesiredCollectorResumeAndIsReported() {
        val actions = mutableListOf<String>()
        val failures = mutableListOf<String>()
        val recovery = PostEnrollmentRecovery(
            collectionDesired = { true },
            setupReady = { true },
            enqueueResidualUpload = { error("work manager unavailable") },
            startCollector = { actions += "start collector" },
            reportFailure = failures::add,
        )

        recovery.resume()

        assertEquals(listOf("start collector"), actions)
        assertEquals(1, failures.size)
        assertEquals(true, failures.single().contains("Buffered upload resume failed"))
    }

    @Test fun failedResumeCheckIsReportedWithoutStartingCollector() {
        val actions = mutableListOf<String>()
        val failures = mutableListOf<String>()
        val recovery = PostEnrollmentRecovery(
            collectionDesired = { throw IllegalStateException() },
            setupReady = { true },
            enqueueResidualUpload = { actions += "enqueue buffered upload" },
            startCollector = { actions += "start collector" },
            reportFailure = failures::add,
        )

        recovery.resume()

        assertEquals(listOf("enqueue buffered upload"), actions)
        assertEquals(1, failures.size)
        assertEquals(true, failures.single().contains("IllegalStateException"))
    }

    private fun recovery(
        desired: Boolean,
        ready: Boolean,
        enqueue: () -> Unit,
        start: () -> Unit,
    ) = PostEnrollmentRecovery(
        collectionDesired = { desired },
        setupReady = { ready },
        enqueueResidualUpload = enqueue,
        startCollector = start,
        reportFailure = { error(it) },
    )
}
