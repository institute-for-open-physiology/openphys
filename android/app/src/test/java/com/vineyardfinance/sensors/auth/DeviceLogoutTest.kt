package com.vineyardfinance.sensors.auth

import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Test

class DeviceLogoutTest {
    @Test fun onlyClearsCredentialWhenStoppedAndFullyUploaded() = runTest {
        var cleared = false
        DeviceLogout({ false }, { false }, { cleared = true }).logOut()
        assertTrue(cleared)
    }
    @Test fun refusesActiveRecordingWithoutQueryingQueue() = runTest {
        val error = runCatching { DeviceLogout({ true }, { error("must not query") }, { error("must not clear") }).logOut() }.exceptionOrNull()
        assertTrue(error!!.message!!.startsWith("Stop recording"))
    }
    @Test fun refusesPendingDataAndUnreadableQueueWithoutClearingCredential() = runTest {
        for (unreadable in listOf(false, true)) {
            var cleared = false
            val result = runCatching { DeviceLogout({ false }, { if (unreadable) error("disk failed") else true }, { cleared = true }).logOut() }
            assertTrue(result.isFailure)
            assertFalse(cleared)
        }
    }
    @Test fun recordingStartedDuringQueueReadPreventsLogout() = runTest {
        var recording = false
        var cleared = false
        val result = runCatching {
            DeviceLogout({ recording }, { recording = true; false }, { cleared = true }).logOut()
        }
        assertTrue(result.isFailure)
        assertFalse(cleared)
    }
}
