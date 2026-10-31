package com.vineyardfinance.sensors.collector

import kotlin.math.abs

fun selectPoint(points: List<SignalPoint>, time: Double, tolerance: Double): SignalPoint? =
    points
        .minByOrNull { abs(it.time - time) }
        ?.takeIf {
            it.value != null && abs(it.time - time) <= tolerance
        }
