package com.vineyardfinance.sensors.ui

import android.graphics.Paint
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.*
import androidx.compose.ui.graphics.drawscope.*
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.*
import androidx.compose.ui.unit.dp
import com.vineyardfinance.sensors.collector.SignalPoint
import com.vineyardfinance.sensors.collector.selectPoint
import com.vineyardfinance.sensors.data.SensorEvent
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.math.*

fun formatTime(time: Double): String =
    Instant.ofEpochMilli((time * 1000).toLong())
        .atZone(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("HH:mm"))

private fun positionEvent(
    time: Double,
    start: Double,
    end: Double,
    width: Float,
    density: Float,
): Float =
    (34 * density + (time - start) / (end - start) * (width - 38 * density))
        .toFloat()
        .coerceIn(66 * density, width - 48 * density)

enum class PlotStyle {
    Line,
    Dots,
    Bars,
}

data class PlotBand(val low: Double, val high: Double, val color: Color)

@Composable
fun SignalPlot(
    points: List<SignalPoint>,
    start: Double,
    end: Double,
    color: Color,
    modifier: Modifier = Modifier,
    fixedRange: ClosedFloatingPointRange<Double>? = null,
    style: PlotStyle = PlotStyle.Line,
    bands: List<PlotBand> = emptyList(),
    events: List<SensorEvent> = emptyList(),
    onEvent: (SensorEvent) -> Unit = {},
    showExtremes: Boolean = false,
    selection: Double? = null,
    onSelection: (Double?) -> Unit = {},
) {
    val currentStart by rememberUpdatedState(start)
    val currentEnd by rememberUpdatedState(end)
    val currentEvents by rememberUpdatedState(events)
    val select by rememberUpdatedState(onSelection)
    val edit by rememberUpdatedState(onEvent)
    val colors = palette
    val density = LocalDensity.current
    val textSize = with(density) { 10f * fontScale * this.density }
    val visible = points.filter { it.time in start..end }
    val values = visible.mapNotNull { it.value }
    val padding = max(5.0, ((values.maxOrNull() ?: 100.0) - (values.minOrNull() ?: 0.0)) * .15)
    val low =
        fixedRange?.start
            ?: if (style == PlotStyle.Bars) 0.0 else (values.minOrNull() ?: 0.0) - padding
    val high = fixedRange?.endInclusive ?: (values.maxOrNull() ?: 100.0) + padding
    val axis = with(density) { 34.dp.toPx() }
    Canvas(
        modifier
            .semantics { contentDescription = "Signal graph. ${values.size} readings." }
            .pointerInput(Unit) {
                detectHorizontalDragGestures(
                    onDragEnd = { select(null) },
                    onDragCancel = { select(null) },
                ) { change, _ ->
                    change.consume()
                    select(
                        currentStart +
                            ((change.position.x - axis) / (size.width - axis)).coerceIn(0f, 1f) *
                                (currentEnd - currentStart)
                    )
                }
            }
            .pointerInput(Unit) {
                detectTapGestures { position ->
                    fun labelX(event: SensorEvent) =
                        positionEvent(
                            event.time,
                            currentStart,
                            currentEnd,
                            size.width.toFloat(),
                            density.density,
                        )
                    val event =
                        currentEvents
                            .filter { it.time in currentStart..currentEnd }
                            .take(6)
                            .minByOrNull { abs(labelX(it) - position.x) }
                    if (event != null && abs(labelX(event) - position.x) < 48 * density.density)
                        edit(event)
                }
            }
    ) {
        val top = if (showExtremes) 42.dp.toPx() else 12.dp.toPx()
        val bottom = size.height - 24.dp.toPx()
        val width = size.width - axis - 4.dp.toPx()
        fun x(time: Double) = axis + ((time - start) / (end - start) * width).toFloat()
        fun y(value: Double) = bottom - ((value - low) / (high - low) * (bottom - top)).toFloat()
        val paint =
            Paint(Paint.ANTI_ALIAS_FLAG).apply {
                this.textSize = textSize
                this.color = colors.muted.toArgb()
            }
        bands.forEach { band ->
            val a = y(band.high.coerceIn(low, high))
            val b = y(band.low.coerceIn(low, high))
            drawRect(
                band.color.copy(alpha = 0.045f),
                Offset(axis, a),
                androidx.compose.ui.geometry.Size(width, b - a),
            )
        }
        listOf(low, (low + high) / 2, high).forEach { value ->
            val py = y(value)
            drawLine(colors.line, Offset(axis, py), Offset(size.width, py), 0.5.dp.toPx())
            drawContext.canvas.nativeCanvas.drawText(
                value.roundToInt().toString(),
                0f,
                py + textSize / 3,
                paint,
            )
        }
        for (fraction in listOf(0.0, 0.5, 1.0)) {
            val label =
                if (end - start >= 86400)
                    Instant.ofEpochMilli(((start + (end - start) * fraction) * 1000).toLong())
                        .atZone(ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("MMM d"))
                else formatTime(start + (end - start) * fraction)
            val px =
                axis + width * fraction.toFloat() - paint.measureText(label) * fraction.toFloat()
            drawContext.canvas.nativeCanvas.drawText(label, px, size.height - 2.dp.toPx(), paint)
        }
        clipRect(axis, top - 4.dp.toPx(), size.width, bottom + 2.dp.toPx()) {
            var last: SignalPoint? = null
            val path = Path()
            visible.forEach { point ->
                val value = point.value
                if (value == null) {
                    last = null
                    return@forEach
                }
                val px = x(point.time)
                val py = y(value)
                when (style) {
                    PlotStyle.Line -> {
                        if (
                            last == null ||
                                point.time - last!!.time > max(7.5, (end - start) / 240 * 1.5)
                        )
                            path.moveTo(px, py)
                        else path.lineTo(px, py)
                        last = point
                    }
                    PlotStyle.Dots ->
                        drawCircle(color.copy(alpha = .7f), 1.7.dp.toPx(), Offset(px, py))
                    PlotStyle.Bars ->
                        drawLine(
                            color.copy(alpha = .65f),
                            Offset(px, bottom),
                            Offset(px, py),
                            max(1.dp.toPx(), width / 260),
                            StrokeCap.Round,
                        )
                }
            }
            if (style == PlotStyle.Line)
                drawPath(
                    path,
                    color,
                    style = Stroke(1.7.dp.toPx(), cap = StrokeCap.Round, join = StrokeJoin.Round),
                )
            if (values.isNotEmpty() && style != PlotStyle.Bars) {
                val avg = values.average()
                drawLine(
                    colors.faint,
                    Offset(axis, y(avg)),
                    Offset(size.width, y(avg)),
                    1.dp.toPx(),
                    pathEffect = PathEffect.dashPathEffect(floatArrayOf(5.dp.toPx(), 5.dp.toPx())),
                )
            }
        }
        if (showExtremes && values.isNotEmpty()) {
            val avg = values.average()
            val label = "Avg ${formatValue(avg)}"
            drawContext.canvas.nativeCanvas.drawText(
                label,
                size.width - paint.measureText(label),
                y(avg) - 7.dp.toPx(),
                paint,
            )
            if (selection == null) {
                val samples = visible.filter { it.value != null }
                listOf(
                        samples.minByOrNull { it.value!! } to "low",
                        samples.maxByOrNull { it.value!! } to "high",
                    )
                    .forEach { (point, suffix) ->
                        point?.let {
                            val value = it.value!!
                            val text = "${formatValue(value)} $suffix"
                            val px = x(it.time)
                            val py = y(value)
                            drawCircle(color, 2.dp.toPx(), Offset(px, py))
                            drawContext.canvas.nativeCanvas.drawText(
                                text,
                                (px - paint.measureText(text) / 2).coerceIn(
                                    axis,
                                    size.width - paint.measureText(text),
                                ),
                                py + if (suffix == "high") -9.dp.toPx() else 16.dp.toPx(),
                                paint,
                            )
                        }
                    }
            }
        }
        if (values.isEmpty()) {
            val label = "No data in this range"
            drawContext.canvas.nativeCanvas.drawText(
                label,
                (axis + size.width - paint.measureText(label)) / 2,
                (top + bottom) / 2,
                paint,
            )
        }
        events
            .filter { it.time in start..end }
            .take(6)
            .forEach { event ->
                val px = x(event.time)
                drawLine(
                    colors.faint.copy(alpha = .5f),
                    Offset(px, top),
                    Offset(px, bottom),
                    .7.dp.toPx(),
                )
                val label = "• " + event.label.take(14)
                val center = positionEvent(event.time, start, end, size.width, density.density)
                paint.color = colors.ink.toArgb()
                drawContext.canvas.nativeCanvas.drawText(
                    label,
                    center - paint.measureText(label) / 2,
                    12.dp.toPx(),
                    paint,
                )
                paint.color = colors.muted.toArgb()
                val stamp =
                    Instant.ofEpochMilli((event.time * 1000).toLong())
                        .atZone(ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
                drawContext.canvas.nativeCanvas.drawText(
                    stamp,
                    center - paint.measureText(stamp) / 2,
                    27.dp.toPx(),
                    paint,
                )
            }
        selection?.let { stamp ->
            val px = x(stamp)
            drawLine(colors.ink.copy(alpha = .6f), Offset(px, top), Offset(px, bottom), 1.dp.toPx())
            selectPoint(visible, stamp, max(3.0, (end - start) / 200))?.let {
                drawCircle(color, 4.dp.toPx(), Offset(px, y(it.value!!)))
            }
        }
    }
}

@Composable
fun ECGPlot(points: List<SignalPoint>, time: Double, modifier: Modifier = Modifier) {
    val colors = palette
    Canvas(modifier.semantics { contentDescription = "Live ECG" }) {
        val end = time - .9
        val start = end - 4
        val visible = points.filter { it.time in start..end }
        val low = min(-300.0, visible.mapNotNull { it.value }.minOrNull() ?: -300.0)
        val high = max(1200.0, visible.mapNotNull { it.value }.maxOrNull() ?: 1200.0)
        val left = 30.dp.toPx()
        val bottom = size.height - 28.dp.toPx()
        val width = size.width - left
        fun y(value: Double) =
            (bottom - (value - low) / (high - low) * (bottom - 8.dp.toPx())).toFloat()
        for (i in 0..20) {
            val x = left + width * i / 20
            drawLine(colors.line.copy(alpha = .45f), Offset(x, 0f), Offset(x, bottom), .5.dp.toPx())
        }
        for (i in 0..6) drawLine(
            colors.line.copy(alpha = .45f),
            Offset(left, bottom * i / 6),
            Offset(size.width, bottom * i / 6),
            .5.dp.toPx(),
        )
        val path = Path()
        var last: SignalPoint? = null
        visible.forEach { point ->
            val value = point.value ?: return@forEach
            val x = left + ((point.time - start) / 4 * width).toFloat()
            if (last == null || point.time - last!!.time > .05) path.moveTo(x, y(value))
            else path.lineTo(x, y(value))
            last = point
        }
        drawPath(
            path,
            colors.ink,
            style = Stroke(1.4.dp.toPx(), cap = StrokeCap.Round, join = StrokeJoin.Round),
        )
        val paint =
            Paint(Paint.ANTI_ALIAS_FLAG).apply {
                color = colors.muted.toArgb()
                textSize = 10.dp.toPx()
            }
        drawContext.canvas.nativeCanvas.drawText("1 mV", 0f, y(1000.0), paint)
        drawContext.canvas.nativeCanvas.drawText("0", 12.dp.toPx(), y(0.0), paint)
        drawLine(
            colors.faint,
            Offset(size.width - width / 4, size.height - 14.dp.toPx()),
            Offset(size.width, size.height - 14.dp.toPx()),
            1.dp.toPx(),
        )
        drawContext.canvas.nativeCanvas.drawText(
            "1 s",
            size.width - width / 8 - 8.dp.toPx(),
            size.height,
            paint,
        )
    }
}
