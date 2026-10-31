package com.vineyardfinance.sensors.ui

import androidx.compose.foundation.Canvas
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.*
import androidx.compose.ui.graphics.*
import androidx.compose.ui.graphics.drawscope.*
import androidx.compose.ui.semantics.*

enum class Icon {
    Heart,
    History,
    Device,
    Cloud,
    Server,
    Phone,
    Info,
    Plus,
    Battery,
    Signal,
    Clock,
    Chevron,
    Close,
}

@Composable
fun AppIcon(
    icon: Icon,
    modifier: Modifier = Modifier,
    color: Color = palette.ink,
    filled: Boolean = false,
) {
    Canvas(modifier.semantics { contentDescription = icon.name }) {
        val stroke = Stroke(width = 1.7f, cap = StrokeCap.Round, join = StrokeJoin.Round)
        scale(size.width / 24, size.height / 24, Offset.Zero) {
            fun line(x: Float, y: Float, a: Float, b: Float) =
                drawLine(
                    color,
                    Offset(x, y),
                    Offset(a, b),
                    strokeWidth = 1.7f,
                    cap = StrokeCap.Round,
                )
            when (icon) {
                Icon.Heart -> {
                    val path =
                        Path().apply {
                            moveTo(12f, 21f)
                            cubicTo(9f, 18f, 2f, 13f, 2f, 7.5f)
                            cubicTo(2f, 1.5f, 9f, 0.8f, 12f, 6f)
                            cubicTo(15f, 0.8f, 22f, 1.5f, 22f, 7.5f)
                            cubicTo(22f, 13f, 15f, 18f, 12f, 21f)
                            close()
                        }
                    drawPath(path, color, style = if (filled) Fill else stroke)
                }
                Icon.Device -> {
                    drawRoundRect(
                        color,
                        Offset(4f, 5.4f),
                        Size(16f, 13.2f),
                        CornerRadius(6f),
                        style = stroke,
                    )
                    line(0f, 12f, 4f, 12f)
                    line(20f, 12f, 24f, 12f)
                }
                Icon.History,
                Icon.Clock -> {
                    drawArc(
                        color,
                        220f,
                        320f,
                        false,
                        Offset(3f, 3f),
                        Size(18f, 18f),
                        style = stroke,
                    )
                    line(12f, 7f, 12f, 12f)
                    line(12f, 12f, 16f, 14f)
                    if (icon == Icon.History) {
                        line(2f, 3f, 2f, 8f)
                        line(2f, 8f, 7f, 8f)
                    }
                }
                Icon.Cloud ->
                    drawPath(
                        Path().apply {
                            moveTo(6f, 19f)
                            cubicTo(-1f, 19f, 0f, 9f, 6f, 9f)
                            cubicTo(6f, 1f, 18f, 1f, 19f, 10f)
                            cubicTo(25f, 10f, 25f, 19f, 19f, 19f)
                            close()
                        },
                        color,
                        style = stroke,
                    )
                Icon.Server -> {
                    drawRoundRect(
                        color,
                        Offset(2f, 2f),
                        Size(20f, 20f),
                        CornerRadius(2f),
                        style = stroke,
                    )
                    line(2f, 9f, 22f, 9f)
                    line(2f, 16f, 22f, 16f)
                    for (y in listOf(5.5f, 12.5f, 19f)) drawCircle(color, 0.8f, Offset(18f, y))
                }
                Icon.Phone -> {
                    drawRoundRect(
                        color,
                        Offset(6f, 1f),
                        Size(12f, 22f),
                        CornerRadius(2f),
                        style = stroke,
                    )
                    line(10f, 4f, 14f, 4f)
                    line(10f, 20f, 14f, 20f)
                }
                Icon.Info -> {
                    drawCircle(color, 10f, Offset(12f, 12f), style = stroke)
                    line(12f, 11f, 12f, 17f)
                    drawCircle(color, 1f, Offset(12f, 7f))
                }
                Icon.Plus -> {
                    line(4f, 12f, 20f, 12f)
                    line(12f, 4f, 12f, 20f)
                }
                Icon.Close -> {
                    line(6f, 6f, 18f, 18f)
                    line(6f, 18f, 18f, 6f)
                }
                Icon.Battery -> {
                    drawRoundRect(
                        color,
                        Offset(1f, 6f),
                        Size(19f, 12f),
                        CornerRadius(2f),
                        style = stroke,
                    )
                    line(23f, 10f, 23f, 14f)
                    drawRect(color, Offset(4f, 9f), Size(9f, 6f))
                }
                Icon.Signal -> {
                    for (i in 0..3) line(4f + i * 5, 20f, 4f + i * 5, 17f - i * 4)
                }
                Icon.Chevron -> {
                    line(9f, 6f, 15f, 12f)
                    line(15f, 12f, 9f, 18f)
                }
            }
        }
    }
}
