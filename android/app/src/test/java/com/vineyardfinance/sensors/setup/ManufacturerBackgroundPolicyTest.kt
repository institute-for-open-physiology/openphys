package com.vineyardfinance.sensors.setup

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ManufacturerBackgroundPolicyTest {
    @Test fun samsungKeepsDeviceCareButtonsAndGuidance() {
        val policy = ManufacturerBackgroundPolicy.forManufacturer("Samsung")
        assertTrue(policy.required)
        assertTrue(policy.samsungDeviceCare)
        assertEquals("https://dontkillmyapp.com/samsung", policy.helpUrl)
        assertTrue(policy.incompleteDetail.contains("Open Never sleeping apps settings"))
        assertTrue(policy.incompleteDetail.contains("manual path: Settings → Battery and device care"))
    }

    @Test fun aggressiveVendorsGetManualPathAndInstructionsLinkWithoutButtons() {
        for ((manufacturer, slug, label) in listOf(
            Triple("xiaomi", "xiaomi", "Xiaomi/Redmi/POCO"),
            Triple("Redmi", "xiaomi", "Xiaomi/Redmi/POCO"),
            Triple("POCO", "xiaomi", "Xiaomi/Redmi/POCO"),
            Triple("HUAWEI", "huawei", "Huawei/Honor"),
            Triple("honor", "huawei", "Huawei/Honor"),
            Triple("OPPO", "oppo", "Oppo/realme"),
            Triple("realme", "oppo", "Oppo/realme"),
            Triple("OnePlus", "oneplus", "OnePlus"),
            Triple("vivo", "vivo", "vivo/iQOO"),
            Triple("iQOO", "vivo", "vivo/iQOO"),
            Triple("Meizu", "meizu", "Meizu"),
        )) {
            val policy = ManufacturerBackgroundPolicy.forManufacturer(manufacturer)
            assertTrue(manufacturer, policy.required)
            assertFalse(manufacturer, policy.samsungDeviceCare)
            assertEquals(manufacturer, "https://dontkillmyapp.com/$slug", policy.helpUrl)
            assertEquals(manufacturer, label, policy.label)
            assertTrue(manufacturer, policy.incompleteDetail.contains("Training Sensors"))
            assertTrue(
                manufacturer,
                policy.incompleteDetail.contains("Open background-settings instructions"),
            )
            assertTrue(manufacturer, policy.confirmedDetail.contains("confirmed by user"))
        }
    }

    @Test fun unknownManufacturersAreHonestlyNotRequired() {
        for (manufacturer in listOf("Google", "motorola", "Sony", "Fairphone", "")) {
            val policy = ManufacturerBackgroundPolicy.forManufacturer(manufacturer)
            assertFalse(manufacturer, policy.required)
            assertFalse(manufacturer, policy.samsungDeviceCare)
            assertNull(manufacturer, policy.helpUrl)
            assertTrue(
                manufacturer,
                policy.confirmedDetail.contains("No additional manufacturer background step"),
            )
        }
    }
}
