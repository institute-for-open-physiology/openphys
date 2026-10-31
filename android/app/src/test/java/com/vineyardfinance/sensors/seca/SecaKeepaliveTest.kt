package com.vineyardfinance.sensors.seca

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SecaKeepaliveTest {
    @Test fun emitsReadOnlyQueriesEveryTwoSecondsUntilConnectionCancellation() = runTest {
        val sent = mutableListOf<ByteArray>()
        val job = launch { runSecaKeepalive { sent += SecaProtocol.command("", read = true) } }
        runCurrent(); advanceTimeBy(1999); runCurrent()
        assertTrue(sent.isEmpty())
        advanceTimeBy(1); runCurrent(); assertEquals(1, sent.size)
        advanceTimeBy(4000); runCurrent(); assertEquals(3, sent.size)
        assertTrue(sent.all { it.contentEquals(SecaProtocol.command("", read = true)) })
        job.cancel(); runCurrent(); advanceTimeBy(10_000); runCurrent()
        assertEquals(3, sent.size)
    }
}
