package com.vineyardfinance.sensors.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.vineyardfinance.sensors.collector.SignalPoint
import com.vineyardfinance.sensors.collector.selectPoint
import com.vineyardfinance.sensors.data.mergeCurrentBin
import kotlin.math.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

@Composable
fun HistoryView(state: MobileState, model: MobileModel?) {
    var zoom by remember { mutableFloatStateOf(ln(600f)) }
    var pastEnd by remember { mutableStateOf<Double?>(null) }
    var selection by remember { mutableStateOf<Double?>(null) }
    var bins by remember { mutableStateOf(state.recent) }
    var error by remember { mutableStateOf<String?>(null) }
    val span = round(exp(zoom.toDouble()))
    val end = pastEnd ?: floor(state.time)
    val start = end - span
    LaunchedEffect(start, end, model) {
        if (model != null)
            withContext(Dispatchers.IO) {
                try {
                    val saved =
                        model.graph.signalStore.readBins(
                            start,
                            end,
                            min(240, max(2, (span / 5).toInt())),
                        )
                    val current = model.graph.signals.currentBin()?.takeIf { it.time in start..end }
                    bins = mergeCurrentBin(saved, current, start, end)
                    error = null
                } catch (_: Exception) {
                    error = "History could not be opened."
                }
            }
    }
    Page(MobileScreen.History.title) {
        Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                IconButton(
                    onClick = {
                        pastEnd = end - span
                        selection = null
                    }
                ) {
                    AppIcon(Icon.Chevron, Modifier.size(20.dp).graphicsLayer { rotationZ = 180f })
                }
                Column(Modifier.weight(1f), horizontalAlignment = Alignment.CenterHorizontally) {
                    Text(formatSpan(span), fontSize = 17.sp)
                    Text(
                        "${formatTime(start)} to ${formatTime(end)}",
                        fontSize = 11.sp,
                        color = palette.muted,
                    )
                }
                IconButton(
                    onClick = {
                        pastEnd = if (end + span >= state.time) null else end + span
                        selection = null
                    },
                    enabled = pastEnd != null,
                ) {
                    AppIcon(
                        Icon.Chevron,
                        Modifier.size(20.dp),
                        if (pastEnd != null) palette.ink else palette.faint,
                    )
                }
            }
            Slider(
                value = zoom,
                onValueChange = {
                    zoom = it
                    selection = null
                },
                valueRange = ln(300f)..ln(604800f),
                onValueChangeFinished = {
                    val nearest =
                        listOf(600.0, 3600.0, 21600.0, 86400.0, 604800.0).minBy {
                            abs(ln(it) - zoom)
                        }
                    if (abs(ln(nearest) - zoom) < .16) zoom = ln(nearest).toFloat()
                },
            )
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                listOf("5m", "1h", "1d", "1w").forEach {
                    Text(it, fontSize = 10.sp, color = palette.muted)
                }
            }
            if (pastEnd != null)
                TextButton(
                    onClick = {
                        pastEnd = null
                        selection = null
                    },
                    modifier = Modifier.align(Alignment.CenterHorizontally),
                ) {
                    Text("Back to now")
                }
        }
        val sections =
            listOf(
                HistoryContent.heartRate,
                HistoryContent.rr,
                HistoryContent.activity,
                HistoryContent.rssi,
                HistoryContent.delay,
            )
        sections.forEachIndexed { index, section ->
            val points = bins.map { SignalPoint(it.time, it.values[index]) }
            val current =
                if (selection != null) selectPoint(points, selection!!, span / bins.size / 2)?.value
                else if (pastEnd != null) points.lastOrNull()?.value
                else
                    when (index) {
                        0 -> state.signals.bpm?.toDouble()
                        1 -> state.signals.rr
                        3 -> state.status.rssiDbm?.toDouble().takeIf { state.status.connected }
                        else -> state.signals.current[index].takeIf { state.status.connected }
                    }
            val color =
                when (index) {
                    0,
                    1 -> palette.heart
                    2 -> palette.motion
                    3 -> signalColor(state.status.rssiDbm)
                    else -> palette.warn
                }
            val bands =
                when (index) {
                    3 ->
                        listOf(
                            PlotBand(-100.0, -80.0, palette.bad),
                            PlotBand(-80.0, -65.0, palette.warn),
                            PlotBand(-65.0, -30.0, palette.up),
                        )
                    4 ->
                        listOf(
                            PlotBand(-10000.0, 100.0, palette.up),
                            PlotBand(100.0, 250.0, palette.warn),
                            PlotBand(250.0, 10000.0, palette.bad),
                        )
                    else -> emptyList()
                }
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                SectionTitle(section, formatValue(current, section.unit))
                SignalPlot(
                    points,
                    start,
                    end,
                    color,
                    Modifier.fillMaxWidth().height(Layout.chartHeight.dp),
                    fixedRange = if (index == 3) -100.0..-30.0 else null,
                    style =
                        when (index) {
                            1 -> PlotStyle.Dots
                            2 -> PlotStyle.Bars
                            else -> PlotStyle.Line
                        },
                    bands = bands,
                    selection = selection,
                    onSelection = { selection = it },
                )
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                    val values = points.mapNotNull { it.value }
                    Text(
                        if (index == 2) "Variation in acceleration"
                        else
                            "Average ${formatValue(values.takeIf {it.isNotEmpty()}?.average(),section.unit)}",
                        fontSize = 11.sp,
                        color = palette.muted,
                    )
                    Text(
                        selection?.let(::formatTime) ?: if (pastEnd == null) "Latest" else "",
                        fontSize = 11.sp,
                        color = palette.muted,
                    )
                }
            }
        }
        (error ?: state.signals.error)?.let { Text(it, fontSize = 12.sp, color = palette.bad) }
    }
}

private fun formatSpan(seconds: Double): String =
    when {
        seconds >= 86400 -> "${(seconds/86400*10).roundToInt()/10.0} days"
        seconds >= 3600 -> "${(seconds/3600*10).roundToInt()/10.0} hours"
        else -> "${(seconds/60).roundToInt()} minutes"
    }
