package com.vineyardfinance.sensors.update

/**
 * What a scanned enrollment link means for this installation. A link on a
 * not-yet-connected phone drives the one-use exchange exactly as before; on
 * an already-connected phone it must never be re-exchanged (that would
 * pointlessly rotate the device credential) and instead becomes an update
 * check.
 */
object UpdateDecision {
    sealed interface Action {
        data object ProceedWithConnection : Action
        data class UpdateAvailable(val versionCode: Long, val versionName: String) : Action
        data object AlreadyUpToDate : Action
        data object CheckUnavailable : Action
    }

    fun forScannedLink(
        enrolled: Boolean,
        installedVersionCode: Long,
        serverPreview: AppUpdateClient.ApkPreview?,
    ): Action = when {
        !enrolled -> Action.ProceedWithConnection
        serverPreview == null -> Action.CheckUnavailable
        serverPreview.versionCode > installedVersionCode ->
            Action.UpdateAvailable(serverPreview.versionCode, serverPreview.versionName)
        else -> Action.AlreadyUpToDate
    }
}
