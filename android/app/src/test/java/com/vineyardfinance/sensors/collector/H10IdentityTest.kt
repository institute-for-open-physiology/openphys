package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class H10IdentityTest {
    @Test fun normalizesNonDefaultTargetIntoStableProvenance() {
        assertEquals("A1B2C3D4", H10Identity.configuredSerial(" Polar H10 a1b2c3d4 "))
        assertEquals("Polar H10 A1B2C3D4", H10Identity.advertisedName("a1b2c3d4"))
        assertEquals("h10_a1b2c3d4", H10Identity.physicalDeviceId("A1B2C3D4"))
        assertEquals(
            VerifiedH10Identity("A1B2C3D4", "A1B2C3D4", "h10_a1b2c3d4"),
            H10Identity.verifyObservedSerial("A1B2C3D4", "a1b2c3d4"),
        )
    }

    @Test fun rejectsMissingInvalidOrMismatchedDeviceInformationSerial() {
        listOf(null, "", "different").forEach { observed ->
            assertTrue(runCatching { H10Identity.verifyObservedSerial("A1B2C3D4", observed) }.isFailure)
        }
        assertTrue(runCatching { H10Identity.configuredSerial("Polar H10") }.isFailure)
    }
}
