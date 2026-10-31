package com.vineyardfinance.sensors.seca

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive

/** Host traffic cadence; the transport supplies bounded, journaled writes. */
suspend fun runSecaKeepalive(sendHostTraffic: suspend () -> Unit) {
    while (currentCoroutineContext().isActive) {
        delay(2000)
        sendHostTraffic()
    }
}
