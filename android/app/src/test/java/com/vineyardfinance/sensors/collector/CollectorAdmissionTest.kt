package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.setup.SetupItem
import com.vineyardfinance.sensors.setup.SetupReport
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class CollectorAdmissionTest {
    private val complete = SetupReport(emptyList(), preStartComplete = true, operationalComplete = false)
    private val incomplete = SetupReport(
        listOf(SetupItem("Phone connected", false, "Reconnect this phone")),
        preStartComplete = false,
        operationalComplete = false,
    )

    @Test fun foregroundAdmissionRequiresCredentialPermissionsAndPreStartSetup() {
        assertTrue(CollectorAdmission.prerequisitesSatisfied(true, true, complete))
        assertFalse(CollectorAdmission.prerequisitesSatisfied(false, true, complete))
        assertFalse(CollectorAdmission.prerequisitesSatisfied(true, false, complete))
        assertFalse(CollectorAdmission.prerequisitesSatisfied(true, true, incomplete))
    }

    @Test fun bootResumeAdditionallyRequiresPreviouslyEnabledCollection() {
        assertTrue(CollectorAdmission.shouldResumeAfterBoot(true, true, true, complete))
        assertFalse(CollectorAdmission.shouldResumeAfterBoot(false, true, true, complete))
        assertFalse(CollectorAdmission.shouldResumeAfterBoot(true, false, true, complete))
        assertFalse(CollectorAdmission.shouldResumeAfterBoot(true, true, false, complete))
        assertFalse(CollectorAdmission.shouldResumeAfterBoot(true, true, true, incomplete))
    }

    @Test fun desiredButStoppedCollectorCanStartWithoutFalseStopStartWorkaround() {
        assertTrue(CollectorAdmission.canStartFromUi(serviceRunning = false, setup = complete))
        assertTrue(
            CollectorAdmission.canStopFromUi(
                serviceRunning = false,
                collectionDesired = true,
            ),
        )
        assertFalse(CollectorAdmission.canStartFromUi(serviceRunning = true, setup = complete))
        assertFalse(CollectorAdmission.canStartFromUi(serviceRunning = false, setup = incomplete))
        assertFalse(
            CollectorAdmission.canStopFromUi(
                serviceRunning = false,
                collectionDesired = false,
            ),
        )
    }
}
