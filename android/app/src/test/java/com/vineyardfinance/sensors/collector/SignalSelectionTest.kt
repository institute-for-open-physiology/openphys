package com.vineyardfinance.sensors.collector

import org.junit.Assert.*
import org.junit.Test

class SignalSelectionTest {
    @Test
    fun missingAndStaleReadingsStayEmptyDuringInspection() {
        val points =
            listOf(SignalPoint(100.0, 72.0), SignalPoint(105.0, null), SignalPoint(110.0, 74.0))
        assertNull(selectPoint(emptyList(), 100.0, 3.0))
        assertEquals(72.0, selectPoint(points, 101.0, 3.0)?.value)
        assertNull(selectPoint(points, 105.0, 3.0))
        assertNull(selectPoint(points, 600.0, 3.0))
    }
}
