package com.vineyardfinance.sensors.setup

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SetupChecklistTest {
    @Test fun completeFactsProduceCompleteReport() {
        val report = SetupChecklist.evaluate(completeFacts())
        assertTrue(report.preStartComplete)
        assertTrue(report.operationalComplete)
        assertTrue(report.complete)
        assertEquals(8, report.items.size)
        assertTrue(report.items.all { it.complete })
    }

    @Test fun everyPreStartFactIndependentlyKeepsSetupIncomplete() {
        val cases = listOf(
            completeFacts().copy(credentialAvailable = false) to "Phone connected",
            completeFacts().copy(installationIdValid = false) to "Phone connected",
            completeFacts().copy(bluetoothAvailable = false) to "Bluetooth on",
            completeFacts().copy(bluetoothEnabled = false) to "Bluetooth on",
            completeFacts().copy(nearbyPermissionsGranted = false) to "Nearby devices",
            completeFacts().copy(notificationPermissionGranted = false) to "Notifications",
            completeFacts().copy(notificationsEnabled = false) to "Notifications",
            completeFacts().copy(notificationChannelEnabled = false) to "Notifications",
            completeFacts().copy(batteryOptimizationExempt = false) to "Background battery operation",
            completeFacts().copy(backgroundRestricted = true) to "Background battery operation",
            completeFacts().copy(manufacturerBackgroundConfirmed = false) to "Manufacturer background policy",
            completeFacts().copy(h10TargetValid = false) to "H10 readiness",
        )
        cases.forEach { (facts, expected) ->
            val report = SetupChecklist.evaluate(facts)
            assertFalse(expected, report.preStartComplete)
            assertEquals(expected, report.firstIncomplete?.name)
            assertFalse(report.complete)
        }
    }

    @Test fun samsungConfirmationIsHonestButDoesNotApplyToOtherManufacturers() {
        val samsung = SetupChecklist.evaluate(completeFacts().copy(manufacturerBackgroundConfirmed = false))
        assertFalse(samsung.preStartComplete)
        assertEquals("Manufacturer background policy", samsung.firstIncomplete?.name)
        val detail = samsung.items.single { it.name == "Manufacturer background policy" }.detail
        assertTrue(detail.contains("Open Samsung app settings → Battery → Unrestricted"))
        assertTrue(
            detail.contains(
                "Open Never sleeping apps settings → Background usage limits → " +
                    "Never sleeping apps → add Training Sensors",
            ),
        )
        assertTrue(detail.contains("manual path: Settings → Battery and device care"))
        assertTrue(detail.contains("confirm both steps"))

        val stock = ManufacturerBackgroundPolicy.forManufacturer("google")
        val other = SetupChecklist.evaluate(
            completeFacts().copy(
                manufacturerPolicyRequired = stock.required,
                manufacturerIncompleteDetail = stock.incompleteDetail,
                manufacturerConfirmedDetail = stock.confirmedDetail,
                manufacturerBackgroundConfirmed = false,
            ),
        )
        assertTrue(other.preStartComplete)
        val stockItem = other.items.single { it.name == "Manufacturer background policy" }
        assertTrue(stockItem.complete)
        assertTrue(stockItem.detail.contains("No additional manufacturer background step"))

        val xiaomi = ManufacturerBackgroundPolicy.forManufacturer("Xiaomi")
        val aggressive = SetupChecklist.evaluate(
            completeFacts().copy(
                manufacturerPolicyRequired = xiaomi.required,
                manufacturerIncompleteDetail = xiaomi.incompleteDetail,
                manufacturerConfirmedDetail = xiaomi.confirmedDetail,
                manufacturerBackgroundConfirmed = false,
            ),
        )
        assertFalse(aggressive.preStartComplete)
        val aggressiveItem =
            aggressive.items.single { it.name == "Manufacturer background policy" }
        assertFalse(aggressiveItem.complete)
        assertTrue(aggressiveItem.detail.contains("Xiaomi/Redmi/POCO"))
        assertTrue(aggressiveItem.detail.contains("Open background-settings instructions"))
    }

    @Test fun postStartFactsNeverCreateCircularPreStartGate() {
        val stopped = SetupChecklist.evaluate(
            completeFacts().copy(
                collectionEnabled = false,
                collectorConnected = false,
                ecgFresh = false,
                accFresh = false,
                hrFresh = false,
                cloudAckFresh = false,
            ),
        )
        assertTrue(stopped.preStartComplete)
        assertFalse(stopped.operationalComplete)
        assertEquals("H10 readiness", stopped.firstIncomplete?.name)

        val noAck = SetupChecklist.evaluate(completeFacts().copy(cloudAckFresh = false))
        assertTrue(noAck.preStartComplete)
        assertFalse(noAck.operationalComplete)
        assertEquals("End-to-end health", noAck.firstIncomplete?.name)
    }

    @Test fun everyStreamAndStorageHealthFactIsVisibleAfterStart() {
        listOf(
            completeFacts().copy(collectionEnabled = false),
            completeFacts().copy(collectorConnected = false),
            completeFacts().copy(ecgFresh = false),
            completeFacts().copy(accFresh = false),
            completeFacts().copy(hrFresh = false),
            completeFacts().copy(storageHealthy = false),
            completeFacts().copy(cloudAckFresh = false),
        ).forEach { facts ->
            val report = SetupChecklist.evaluate(facts)
            assertTrue(report.preStartComplete)
            assertFalse(report.operationalComplete)
            assertFalse(report.complete)
        }
    }

    private fun completeFacts() = SetupFacts(
        credentialAvailable = true,
        installationIdValid = true,
        bluetoothAvailable = true,
        bluetoothEnabled = true,
        nearbyPermissionsGranted = true,
        notificationPermissionGranted = true,
        notificationsEnabled = true,
        notificationChannelEnabled = true,
        batteryOptimizationExempt = true,
        backgroundRestricted = false,
        manufacturerPolicyRequired = true,
        manufacturerIncompleteDetail =
            ManufacturerBackgroundPolicy.forManufacturer("samsung").incompleteDetail,
        manufacturerConfirmedDetail =
            ManufacturerBackgroundPolicy.forManufacturer("samsung").confirmedDetail,
        manufacturerBackgroundConfirmed = true,
        h10TargetValid = true,
        collectionEnabled = true,
        collectorConnected = true,
        ecgFresh = true,
        accFresh = true,
        hrFresh = true,
        storageHealthy = true,
        cloudAckFresh = true,
    )
}
