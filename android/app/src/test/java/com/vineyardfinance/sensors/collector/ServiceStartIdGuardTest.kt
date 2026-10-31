package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ServiceStartIdGuardTest {
    @Test
    fun unseenNewerPlatformStartRejectsStopUntilCommandIsDelivered() {
        var platformLatestStartId = 2
        val attemptedIds = mutableListOf<Int>()
        val guard = ServiceStartIdGuard { attemptedId ->
            attemptedIds += attemptedId
            attemptedId == platformLatestStartId
        }
        guard.commandDelivered(1)

        assertFalse(guard.stopIfLatest())
        assertEquals(listOf(1), attemptedIds)

        guard.commandDelivered(2)
        assertTrue(guard.stopIfLatest())
        assertEquals(listOf(1, 2), attemptedIds)

        platformLatestStartId = 3
        assertFalse(guard.stopIfLatest())
        assertEquals(listOf(1, 2, 2), attemptedIds)
    }

    @Test
    fun stopAlwaysUsesLatestDeliveredCommandIdentifier() {
        var attemptedId: Int? = null
        val guard = ServiceStartIdGuard {
            attemptedId = it
            true
        }

        assertFalse(guard.stopIfLatest())
        assertEquals(null, attemptedId)
        guard.commandDelivered(7)
        guard.commandDelivered(9)

        assertTrue(guard.stopIfLatest())
        assertEquals(9, attemptedId)
    }
}
