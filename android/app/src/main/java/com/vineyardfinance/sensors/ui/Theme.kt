package com.vineyardfinance.sensors.ui

import androidx.compose.foundation.*
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.luminance
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

val LocalPalette = staticCompositionLocalOf { mobileColors(true) }
val palette: MobileColors
    @Composable get() = LocalPalette.current

@Composable
fun MobileTheme(dark: Boolean = isSystemInDarkTheme(), content: @Composable () -> Unit) {
    val colors = mobileColors(dark)
    val scheme = if (dark) darkColorScheme() else lightColorScheme()
    CompositionLocalProvider(LocalPalette provides colors) {
        MaterialTheme(
            colorScheme =
                scheme.copy(
                    primary = colors.ink,
                    onPrimary = colors.canvas,
                    background = colors.canvas,
                    onBackground = colors.ink,
                    surface = colors.surface,
                    onSurface = colors.ink,
                    surfaceVariant = colors.raised,
                    onSurfaceVariant = colors.muted,
                    outline = colors.line,
                    error = colors.bad,
                ),
            content = content,
        )
    }
}

@Composable
fun Page(title: String? = null, content: @Composable ColumnScope.() -> Unit) {
    Column(
        Modifier.fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(horizontal = Layout.pageInset.dp)
            .padding(top = 12.dp, bottom = 32.dp),
        verticalArrangement = Arrangement.spacedBy(Layout.sectionGap.dp),
    ) {
        if (title != null)
            Text(
                title,
                fontSize = Layout.titleSize.sp,
                fontWeight = FontWeight.Bold,
                color = palette.ink,
            )
        content()
    }
}

@Composable
fun SectionTitle(
    section: Section,
    value: String? = null,
    action: (@Composable () -> Unit)? = null,
) {
    var showInfo by remember { mutableStateOf(false) }
    Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
        Text(
            section.title,
            fontSize = Layout.sectionTitleSize.sp,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.testTag("section-${section.title}"),
        )
        if (section.info != null)
            TextButton(
                onClick = { showInfo = true },
                contentPadding = PaddingValues(0.dp),
                modifier = Modifier.size(44.dp),
            ) {
                AppIcon(Icon.Info, Modifier.size(15.dp), palette.muted)
            }
        Spacer(Modifier.weight(1f))
        if (value != null) Text(value, fontSize = 14.sp, fontWeight = FontWeight.Medium)
        action?.invoke()
    }
    if (showInfo)
        AlertDialog(
            onDismissRequest = { showInfo = false },
            title = { Text(section.title) },
            text = { Text(section.info.orEmpty()) },
            confirmButton = { TextButton(onClick = { showInfo = false }) { Text("OK") } },
        )
}

@Composable
fun ActionButton(
    label: String,
    enabled: Boolean = true,
    destructive: Boolean = false,
    onClick: () -> Unit,
) {
    val background =
        if (destructive) palette.bad.copy(alpha = 0.12f)
        else if (palette.canvas.luminance() < .5f) Color.White else Color.Black
    val foreground =
        if (destructive) palette.bad
        else if (palette.canvas.luminance() < .5f) Color.Black else Color.White
    Button(
        onClick = onClick,
        enabled = enabled,
        modifier = Modifier.fillMaxWidth().heightIn(min = Layout.buttonHeight.dp),
        shape = RoundedCornerShape(Layout.buttonRadius.dp),
        colors =
            ButtonDefaults.buttonColors(containerColor = background, contentColor = foreground),
    ) {
        Text(label, fontSize = 15.sp, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
fun StatusDot(color: Color, label: String, modifier: Modifier = Modifier) {
    Canvas(modifier.size(Layout.statusDot.dp).testTag(label)) { drawCircle(color) }
}

fun formatValue(value: Double?, unit: String = ""): String =
    value?.takeIf(Double::isFinite)?.let {
        "${kotlin.math.round(it).toInt()}${if (unit.isEmpty()) "" else " $unit"}"
    } ?: "--"

@Composable
fun batteryColor(value: Int?): Color =
    when {
        value == null -> palette.faint
        value <= 15 -> palette.bad
        value <= 30 -> palette.warn
        else -> palette.up
    }

@Composable
fun signalColor(value: Int?): Color =
    when {
        value == null -> palette.faint
        value < -80 -> palette.bad
        value < -65 -> palette.warn
        else -> palette.up
    }
