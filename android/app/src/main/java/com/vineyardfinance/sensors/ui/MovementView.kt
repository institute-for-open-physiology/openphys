package com.vineyardfinance.sensors.ui

import android.graphics.Paint
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.layout.*
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.*
import androidx.compose.ui.graphics.*
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.vineyardfinance.sensors.collector.SignalSnapshot
import kotlin.math.*

@Composable
fun MovementView(signals: SignalSnapshot) {
    val colors = palette
    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
        SectionTitle(
            NowContent.movement,
            if (signals.acceleration == null) "--"
            else if (signals.moving) "Tilt uncertain" else "Tilt",
        )
        Canvas(Modifier.fillMaxWidth().height(Layout.movementHeight.dp)) {
            val g = signals.gravity
            val pitch = atan2(g[2], max(1.0, hypot(g[0], g[1])))
            val roll = atan2(g[1], -g[0])
            val scale = min(size.width / 320, 1.dp.toPx())
            fun project(x: Double, y: Double, z: Double): Offset {
                val a = x * cos(roll) - y * sin(roll)
                val b = x * sin(roll) + y * cos(roll)
                val c = b * cos(pitch) - z * sin(pitch)
                val d = b * sin(pitch) + z * cos(pitch)
                val depth = 420 / (420 + c * .4 + d * .5)
                return Offset(
                    (size.width / 2 + a * depth * scale).toFloat(),
                    (size.height / 2 + (c * .48 - d * .85) * depth * scale).toFloat(),
                )
            }
            for (r in listOf(86f, 117f)) drawOval(
                colors.line,
                Offset(size.width / 2 - r * scale, size.height / 2 - r * .56f * scale),
                Size(r * 2 * scale, r * 1.12f * scale),
                style = Stroke(.7.dp.toPx()),
            )
            fun polygon(points: List<Offset>) =
                Path().apply {
                    points.forEachIndexed { i, p ->
                        if (i == 0) moveTo(p.x, p.y) else lineTo(p.x, p.y)
                    }
                    close()
                }
            val top =
                (0..47).map {
                    val angle = it / 48.0 * PI * 2
                    project(cos(angle) * 72, sin(angle) * 36, 8.0)
                }
            val bottom =
                (0..47).map {
                    val angle = it / 48.0 * PI * 2
                    project(cos(angle) * 72, sin(angle) * 36, -8.0)
                }
            val alpha = if (signals.acceleration == null) .3f else if (signals.moving) .65f else 1f
            drawPath(polygon(bottom), Color(0xff212121), alpha = alpha)
            top.indices.forEach { i ->
                val j = (i + 1) % top.size
                drawPath(
                    polygon(listOf(top[i], top[j], bottom[j], bottom[i])),
                    Color(0xff3b3b3b),
                    alpha = alpha,
                )
            }
            drawPath(
                polygon(top),
                Brush.linearGradient(listOf(Color(0xff474747), Color(0xff1f1f1f))),
                alpha = alpha,
            )
            drawPath(polygon(top), Color.White.copy(alpha = .1f), style = Stroke(.6.dp.toPx()))
            val paint =
                Paint(Paint.ANTI_ALIAS_FLAG).apply {
                    color = Color(0xffa6a6a6).toArgb()
                    textSize = 11.dp.toPx()
                    textAlign = Paint.Align.CENTER
                }
            val center = project(0.0, 0.0, 9.0)
            drawContext.canvas.nativeCanvas.drawText("POLAR", center.x, center.y, paint)
        }
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            listOf("X", "Y", "Z").forEachIndexed { i, label ->
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(label, color = colors.muted, fontSize = 12.sp)
                    Text(formatValue(signals.acceleration?.get(i)), fontSize = 12.sp)
                }
            }
            Text("mG", color = colors.muted, fontSize = 12.sp)
        }
    }
}
