package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class CollectorRuntimePolicyTest {
    @Test fun desiredFatalFailureStaysForegroundAndRequiresDeliberateAction() {
        val decision = CollectorRuntimePolicy.afterFatalFailure(collectionDesired = true)
        val status = CollectorRuntimePolicy.failedStatus(CollectorSnapshot(), "queue insert failed")

        assertEquals(CollectorServiceDirective.HOLD_FAILED_FOREGROUND, decision.directive)
        assertTrue(decision.keepForeground)
        assertFalse(decision.stopService)
        assertFalse(decision.startRuntime)
        assertFalse(decision.scheduleResidualUpload)
        assertTrue(status.serviceRunning)
        assertEquals("Collection failed", status.state)
        assertTrue(status.detail.startsWith("Action required:"))
        assertTrue(status.detail.contains("stop, then start collection"))
        assertEquals("queue insert failed", status.lastError)
        assertFalse(status.connected)
    }

    @Test fun latchedFailureRejectsSilentCollectorAndUploaderRestart() {
        val decision = CollectorRuntimePolicy.onStartCommand(failureLatched = true)

        assertEquals(CollectorServiceDirective.HOLD_FAILED_FOREGROUND, decision.directive)
        assertFalse(decision.startRuntime)
        assertFalse(decision.scheduleResidualUpload)
        assertTrue(decision.keepForeground)
    }

    @Test fun userStopRemovesFailedForegroundService() {
        val decision = CollectorRuntimePolicy.onUserStop()

        assertEquals(CollectorServiceDirective.STOP_SERVICE, decision.directive)
        assertTrue(decision.stopService)
        assertFalse(decision.keepForeground)
        assertFalse(decision.startRuntime)
        assertFalse(decision.scheduleResidualUpload)
    }

    @Test fun fatalFailureAfterUserStopDoesNotUploadOrKeepServiceAlive() {
        val decision = CollectorRuntimePolicy.afterFatalFailure(collectionDesired = false)

        assertTrue(decision.stopService)
        assertFalse(decision.keepForeground)
        assertFalse(decision.scheduleResidualUpload)
    }

    @Test fun cleanStopCanScheduleExistingDurableQueueDrain() {
        val decision = CollectorRuntimePolicy.afterCleanStop()

        assertTrue(decision.stopService)
        assertTrue(decision.scheduleResidualUpload)
    }

    @Test fun deliberateStopDoesNotLatchACleanupFailure() {
        val resolution = CollectorRuntimePolicy.resolveCleanup(
            primaryFailure = null,
            cleanupFailure = "network unregister failed",
            collectionDesired = false,
        )

        assertEquals("network unregister failed", resolution.failureDetail)
        assertEquals(null, resolution.fatalCleanupFailure)
    }

    @Test fun desiredCollectionLatchesANewCleanupFailure() {
        val resolution = CollectorRuntimePolicy.resolveCleanup(
            primaryFailure = null,
            cleanupFailure = "network unregister failed",
            collectionDesired = true,
        )

        assertEquals("network unregister failed", resolution.failureDetail)
        assertEquals("network unregister failed", resolution.fatalCleanupFailure)
    }

    @Test fun cleanupFailureDoesNotReplaceOrRelatchAPrimaryFailure() {
        val resolution = CollectorRuntimePolicy.resolveCleanup(
            primaryFailure = "archive failed",
            cleanupFailure = "network unregister failed",
            collectionDesired = true,
        )

        assertEquals("archive failed", resolution.failureDetail)
        assertEquals(null, resolution.fatalCleanupFailure)
    }

    @Test fun normalStartIsAllowedOnlyWithoutAFailureLatch() {
        val decision = CollectorRuntimePolicy.onStartCommand(failureLatched = false)

        assertTrue(decision.startRuntime)
        assertFalse(decision.keepForeground)
        assertFalse(decision.stopService)
    }

}
