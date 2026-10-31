package com.vineyardfinance.sensors.collector

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class CollectorRuntimeSequencerTest {
    private class Token(
        var active: Boolean = true,
        var completed: Boolean = false,
        var cancellations: Int = 0,
    )

    private fun sequencer() = CollectorRuntimeSequencer<Token>(
        isActive = { it.active },
        isCompleted = { it.completed },
        cancel = {
            it.cancellations += 1
            it.active = false
        },
    )

    @Test
    fun startStopStartWaitsForExactOldCompletionBeforeRestart() {
        val sequencer = sequencer()
        val first = Token()
        var launched = 0
        assertEquals(
            CollectorRuntimeSequencer.StartDecision.STARTED,
            sequencer.requestStart { launched += 1; first },
        )
        assertTrue(sequencer.hasActiveRuntime())

        assertEquals(CollectorRuntimeSequencer.StopDecision.WAIT_FOR_COMPLETION, sequencer.requestStop())
        assertEquals(1, first.cancellations)
        assertFalse(sequencer.hasActiveRuntime())
        assertEquals(
            CollectorRuntimeSequencer.StartDecision.DEFERRED,
            sequencer.requestStart { launched += 1; Token() },
        )
        assertEquals(1, launched)

        first.completed = true
        assertEquals(
            CollectorRuntimeSequencer.CompletionDecision.RESTART,
            sequencer.completed(first),
        )
        val second = Token()
        assertEquals(
            CollectorRuntimeSequencer.StartDecision.STARTED,
            sequencer.requestStart { launched += 1; second },
        )
        assertEquals(2, launched)
        assertTrue(sequencer.hasActiveRuntime())
    }

    @Test
    fun secondStopClearsDeferredRestart() {
        val sequencer = sequencer()
        val token = Token()
        sequencer.requestStart { token }
        sequencer.requestStop()
        sequencer.requestStart { Token() }

        assertEquals(CollectorRuntimeSequencer.StopDecision.WAIT_FOR_COMPLETION, sequencer.requestStop())
        assertEquals(2, token.cancellations)
        token.completed = true
        assertEquals(
            CollectorRuntimeSequencer.CompletionDecision.STOP_SERVICE,
            sequencer.completed(token),
        )
    }

    @Test
    fun activeDuplicateStartDoesNotLaunchAgain() {
        val sequencer = sequencer()
        var launches = 0
        sequencer.requestStart { launches += 1; Token() }

        assertEquals(
            CollectorRuntimeSequencer.StartDecision.ALREADY_RUNNING,
            sequencer.requestStart { launches += 1; Token() },
        )
        assertEquals(1, launches)
    }

    @Test
    fun completedTokenCanBeReplacedBeforeItsStaleCallbackArrives() {
        val sequencer = sequencer()
        val old = Token()
        sequencer.requestStart { old }
        old.active = false
        old.completed = true
        val replacement = Token()

        assertEquals(
            CollectorRuntimeSequencer.StartDecision.STARTED,
            sequencer.requestStart { replacement },
        )
        assertEquals(
            CollectorRuntimeSequencer.CompletionDecision.IGNORE,
            sequencer.completed(old),
        )
        assertTrue(sequencer.hasActiveRuntime())
    }

    @Test
    fun normalCurrentCompletionStopsService() {
        val sequencer = sequencer()
        val token = Token()
        sequencer.requestStart { token }
        token.active = false
        token.completed = true

        assertEquals(
            CollectorRuntimeSequencer.CompletionDecision.STOP_SERVICE,
            sequencer.completed(token),
        )
        assertFalse(sequencer.hasActiveRuntime())
        assertEquals(CollectorRuntimeSequencer.StopDecision.STOP_NOW, sequencer.requestStop())
    }

    @Test
    fun closeCancelsActiveTokenAndRejectsLateWork() {
        val sequencer = sequencer()
        val token = Token()
        sequencer.requestStart { token }

        sequencer.close()
        sequencer.close()

        assertEquals(1, token.cancellations)
        assertFalse(sequencer.hasActiveRuntime())
        assertEquals(
            CollectorRuntimeSequencer.StartDecision.CLOSED,
            sequencer.requestStart { Token() },
        )
        token.completed = true
        assertEquals(
            CollectorRuntimeSequencer.CompletionDecision.IGNORE,
            sequencer.completed(token),
        )
    }

    @Test
    fun closeDoesNotRecancelAlreadyCompletedToken() {
        val sequencer = sequencer()
        val token = Token()
        sequencer.requestStart { token }
        token.active = false
        token.completed = true

        sequencer.close()

        assertEquals(0, token.cancellations)
    }
}
