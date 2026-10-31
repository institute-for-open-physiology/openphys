package com.vineyardfinance.sensors.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.Assert.assertTrue
import org.junit.Test

class HttpJsonCancellationTest {
    /**
     * The upload watchdog's cancel must actually tear a stuck request down. With a blocking
     * execute() this join would wait for the server (or a timeout that failed to fire); the
     * enqueue + Call.cancel wiring returns promptly.
     */
    @Test fun postIsPromptlyCancellableWhileServerStalls() {
        val server = MockWebServer().apply { start() }
        try {
            // NO_RESPONSE stalls the client without a server-side sleep (a timed header
            // delay keeps MockWebServer's queue alive past shutdown's grace period).
            server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
            val http = HttpJson(server.url("/").toString().trimEnd('/'))
            val cancelMs = runBlocking {
                val request = launch(Dispatchers.IO) { runCatching { http.post("/stall", "{}") } }
                delay(250)
                val cancelStarted = System.nanoTime()
                request.cancel()
                request.join()
                (System.nanoTime() - cancelStarted) / 1_000_000
            }
            assertTrue(
                "cancel+join took $cancelMs ms; it must not wait out the stalled server",
                cancelMs < 5_000,
            )
        } finally {
            server.shutdown()
        }
    }
}
