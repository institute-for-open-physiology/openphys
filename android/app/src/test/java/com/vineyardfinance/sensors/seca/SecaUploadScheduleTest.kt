package com.vineyardfinance.sensors.seca

import java.io.File
import org.junit.Assert.*
import org.junit.Test

class SecaUploadScheduleTest {
    private fun file(session: String, index: Int) = File("${session}_${index.toString().padStart(10, '0')}.json")
    @Test fun activePriorityAndHistoricalRoundRobinPreserveLowestIndex() {
        val schedule = SecaUploadSchedule()
        val pending = (0..8).flatMap { listOf(file("a", it), file("b", it), file("z", it)) }.reversed().toMutableList()
        val selected = mutableListOf<String>()
        val indices = mutableMapOf<String, Int>()
        repeat(16) {
            val choice = requireNotNull(schedule.select(pending, "z"))
            selected += choice.session
            val expected = indices.getOrDefault(choice.session, 0)
            assertEquals(file(choice.session, expected), choice.file)
            indices[choice.session] = expected + 1
            schedule.acknowledged(choice); pending.remove(choice.file)
        }
        assertEquals(listOf("z","z","z","a","z","z","z","b","z","z","z","a","b","a","b","a"), selected)
    }
    @Test fun retryDoesNotConsumeTurnAndReconnectDoesNotResetBudget() {
        val schedule = SecaUploadSchedule()
        val pending = listOf(file("a", 7), file("z", 9), file("y", 1))
        val first = requireNotNull(schedule.select(pending, "z"))
        repeat(10) { assertEquals(first, schedule.select(pending, "z")) }
        schedule.acknowledged(first)
        repeat(2) { schedule.acknowledged(requireNotNull(schedule.select(pending, "y"))) }
        val historical = requireNotNull(schedule.select(pending, "z"))
        assertFalse(historical.priority); assertEquals("a", historical.session)
        repeat(10) { assertEquals(historical, schedule.select(pending, "z")) }
        schedule.acknowledged(historical)
        assertEquals("z", schedule.select(pending, "z")?.session)
    }
    @Test fun absentActiveAndStoppedCollectorRotateAllSessions() {
        val schedule = SecaUploadSchedule()
        val pending = listOf(file("a", 0), file("b", 0))
        assertNull(schedule.select(emptyList(), "z"))
        val a = requireNotNull(schedule.select(pending, "missing")); assertEquals("a", a.session)
        schedule.acknowledged(a)
        val b = requireNotNull(schedule.select(pending, null)); assertEquals("b", b.session)
        schedule.acknowledged(b)
        assertEquals("a", schedule.select(pending, null)?.session)
    }
    @Test fun activeOnlyDoesNotIdleAndNewBacklogGetsNextTurn() {
        val schedule = SecaUploadSchedule()
        val active = listOf(file("z", 0))
        repeat(10) {
            val chosen = requireNotNull(schedule.select(active, "z")); assertTrue(chosen.priority)
            schedule.acknowledged(chosen)
        }
        assertEquals("a", schedule.select(active + file("a", 0), "z")?.session)
    }
    @Test fun historicalRotationSurvivesRemovalAndInsertion() {
        val schedule = SecaUploadSchedule()
        schedule.acknowledged(requireNotNull(schedule.select(listOf(file("b", 0), file("d", 0)), null)))
        assertEquals("c", schedule.select(listOf(file("a", 0), file("c", 0)), null)?.session)
        schedule.acknowledged(requireNotNull(schedule.select(listOf(file("a", 0), file("c", 0)), null)))
        assertEquals("a", schedule.select(listOf(file("a", 1)), null)?.session)
    }

}
