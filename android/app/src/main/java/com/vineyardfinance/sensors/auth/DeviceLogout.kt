package com.vineyardfinance.sensors.auth

/** Run on the UI dispatcher: recheck recording after the suspending queue read, then clear. */
class DeviceLogout(
    private val recording: () -> Boolean,
    private val pendingData: suspend () -> Boolean,
    private val clearCredential: () -> Unit,
) {
    suspend fun logOut() {
        check(!recording()) { "Stop recording before logging out, or choose Discard local data." }
        check(!pendingData()) { "Recordings are still waiting to upload. Wait until caught up, or choose Discard local data." }
        check(!recording()) { "Recording started while checking uploads. Stop recording and try again." }
        clearCredential()
    }
}
