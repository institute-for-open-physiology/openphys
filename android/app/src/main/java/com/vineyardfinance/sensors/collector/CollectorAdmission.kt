package com.vineyardfinance.sensors.collector

import com.vineyardfinance.sensors.setup.SetupReport

internal object CollectorAdmission {
    fun prerequisitesSatisfied(
        credentialAvailable: Boolean,
        blePermissionsGranted: Boolean,
        setup: SetupReport,
    ): Boolean = credentialAvailable && blePermissionsGranted && setup.preStartComplete

    fun shouldResumeAfterBoot(
        collectionEnabled: Boolean,
        credentialAvailable: Boolean,
        blePermissionsGranted: Boolean,
        setup: SetupReport,
    ): Boolean = collectionEnabled && prerequisitesSatisfied(credentialAvailable, blePermissionsGranted, setup)

    fun canStartFromUi(serviceRunning: Boolean, setup: SetupReport): Boolean =
        !serviceRunning && setup.preStartComplete

    fun canStopFromUi(serviceRunning: Boolean, collectionDesired: Boolean): Boolean =
        serviceRunning || collectionDesired
}
