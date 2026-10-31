package com.vineyardfinance.sensors.ui

import androidx.compose.animation.core.*
import androidx.compose.foundation.*
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.collector.SignalPoint
import com.vineyardfinance.sensors.collector.selectPoint
import com.vineyardfinance.sensors.data.SensorEvent
import kotlin.math.*
import kotlinx.coroutines.isActive

val LocalFrameTime = staticCompositionLocalOf<Double?> { null }

@Composable
fun frameTime(): Double {
    LocalFrameTime.current?.let {
        return it
    }
    var time by remember { mutableDoubleStateOf(System.currentTimeMillis() / 1000.0) }
    val lifecycle = LocalLifecycleOwner.current.lifecycle
    LaunchedEffect(lifecycle) {
        lifecycle.repeatOnLifecycle(Lifecycle.State.RESUMED) {
            var last = 0L
            while (isActive) withFrameNanos { tick ->
                if (tick - last >= 30_000_000) {
                    time = System.currentTimeMillis() / 1000.0
                    last = tick
                }
            }
        }
    }
    return time
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun NowView(state: MobileState, model: MobileModel?, openDevice: () -> Unit) {
    var selection by remember { mutableStateOf<Double?>(null) }
    var editing by remember { mutableStateOf<SensorEvent?>(null) }
    var addingAt by remember { mutableStateOf<Double?>(null) }
    val points =
        state.signals.heartRate
            .filter { it.time in (state.time - 600)..state.time }
            .ifEmpty { state.recent.map { SignalPoint(it.time, it.values[0]) } }
    val selected = selection?.let { stamp -> selectPoint(points, stamp, 3.0)?.value }
    Page {
        Column(verticalArrangement = Arrangement.spacedBy(Layout.contentGap.dp)) {
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                TextButton(onClick = openDevice, contentPadding = PaddingValues(10.dp)) {
                    AppIcon(Icon.Device, Modifier.size(25.dp), palette.muted)
                    Spacer(Modifier.width(9.dp))
                    StatusDot(
                        if (state.status.connected) palette.up else palette.faint,
                        if (state.status.connected) "Connected" else "Disconnected",
                    )
                }
            }
            Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
                Heart(state)
                Row(
                    verticalAlignment = Alignment.Bottom,
                    horizontalArrangement = Arrangement.spacedBy(7.dp),
                ) {
                    Text(
                        formatValue(
                            if (selection != null) selected else state.signals.bpm?.toDouble()
                        ),
                        fontSize = Layout.heartRateSize.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                    Text(
                        "bpm",
                        fontSize = 16.sp,
                        color = palette.muted,
                        modifier = Modifier.padding(bottom = 9.dp),
                    )
                }
                val status =
                    when {
                        !state.status.connected ->
                            if (state.status.serviceRunning) state.status.state
                            else "Connect your H10"
                        state.signals.contact == false -> "Check strap contact"
                        state.signals.bpm == null -> "Waiting for a heartbeat"
                        else -> null
                    }
                if (status != null)
                    Text(
                        status,
                        fontSize = 12.sp,
                        color = palette.muted,
                        modifier = Modifier.padding(top = 8.dp),
                    )
            }
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    if (selection == null) "Last 10 minutes" else formatTime(selection!!),
                    fontSize = 12.sp,
                    color = palette.muted,
                )
                SignalPlot(
                    points,
                    state.time - 600,
                    state.time,
                    palette.heart,
                    Modifier.fillMaxWidth().height(Layout.heartRateChartHeight.dp),
                    selection = selection,
                    onSelection = { selection = it },
                    showExtremes = true,
                    events = state.events,
                    onEvent = { editing = it },
                )
            }
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                SectionTitle(NowContent.ecg)
                val time = frameTime()
                ECGPlot(
                    state.signals.ecg,
                    time,
                    Modifier.fillMaxWidth().height(Layout.chartHeight.dp),
                )
            }
            MovementView(state.signals)
            Column {
                SectionTitle(
                    NowContent.events,
                    action = {
                        TextButton(
                            onClick = { addingAt = state.time },
                            enabled = state.status.connected,
                        ) {
                            AppIcon(Icon.Plus, Modifier.size(14.dp))
                            Spacer(Modifier.width(6.dp))
                            Text("Add", fontWeight = FontWeight.SemiBold)
                        }
                    },
                )
                if (state.events.isEmpty())
                    Text(
                        "Add context to your heartbeat.",
                        fontSize = 13.sp,
                        color = palette.muted,
                        modifier = Modifier.padding(vertical = 12.dp),
                    )
                state.events.take(10).forEach { event ->
                    key(event.id) {
                        val dismiss =
                            rememberSwipeToDismissBoxState(
                                confirmValueChange = { value ->
                                    if (value == SwipeToDismissBoxValue.EndToStart) {
                                        model?.deleteEvent(event.id)
                                        true
                                    } else false
                                }
                            )
                        SwipeToDismissBox(
                            state = dismiss,
                            enableDismissFromStartToEnd = false,
                            backgroundContent = {
                                Box(
                                    Modifier.fillMaxSize()
                                        .background(palette.bad)
                                        .padding(horizontal = 20.dp),
                                    contentAlignment = Alignment.CenterEnd,
                                ) {
                                    Text(
                                        "Delete",
                                        color = androidx.compose.ui.graphics.Color.White,
                                        fontWeight = FontWeight.Medium,
                                    )
                                }
                            },
                        ) {
                            Row(
                                Modifier.fillMaxWidth()
                                    .background(palette.canvas)
                                    .clickable { editing = event }
                                    .padding(vertical = 18.dp),
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(12.dp),
                            ) {
                                StatusDot(palette.faint, "Event")
                                Text(event.label, Modifier.weight(1f), fontSize = 14.sp)
                                Text(
                                    formatTime(event.time),
                                    fontSize = 12.sp,
                                    color = palette.muted,
                                )
                            }
                        }
                    }
                }
            }
        }
    }
    if (addingAt != null || editing != null) {
        val event = editing
        EventForm(
            event,
            onDismiss = {
                addingAt = null
                editing = null
            },
            onSave = { label ->
                model?.saveEvent(label, event?.time ?: addingAt ?: state.time, event?.id)
                addingAt = null
                editing = null
            },
            onDelete = {
                event?.let { model?.deleteEvent(it.id) }
                addingAt = null
                editing = null
            },
        )
    }
}

@Composable
private fun Heart(state: MobileState) {
    val time = frameTime()
    val bpm = state.signals.bpm
    val displayTime = time - .9
    val freshECG = state.signals.ecg.lastOrNull()?.let { time - it.time < 2.5 } == true
    val phase =
        if (freshECG)
            state.signals.beats.lastOrNull { it <= displayTime }?.let { displayTime - it } ?: 1.0
        else if (bpm != null && bpm > 0) displayTime % (60.0 / bpm) else 1.0
    val pulse =
        if (bpm != null)
            exp(-((phase - .06) / .05).pow(2)) + .4 * exp(-((phase - .32) / .07).pow(2))
        else 0.0
    Box(
        Modifier.fillMaxWidth().height(Layout.heartHeight.dp),
        contentAlignment = Alignment.Center,
    ) {
        val heart = palette.heart
        Canvas(Modifier.size(190.dp, 118.dp)) {
            drawCircle(
                androidx.compose.ui.graphics.Brush.radialGradient(
                    listOf(heart.copy(alpha = .09f), heart.copy(alpha = 0f)),
                    radius = size.width / 2,
                ),
                radius = size.width / 2,
            )
        }
        AppIcon(
            Icon.Heart,
            Modifier.size(82.dp).graphicsLayer {
                scaleX = (1 + pulse * .075).toFloat()
                scaleY = scaleX
            },
            heart,
            filled = true,
        )
    }
}

@Composable
private fun EventForm(
    event: SensorEvent?,
    onDismiss: () -> Unit,
    onSave: (String) -> Unit,
    onDelete: () -> Unit,
) {
    var label by remember { mutableStateOf(event?.label.orEmpty()) }
    val focus = remember { FocusRequester() }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(if (event == null) "Add event" else "Rename event") },
        text = {
            OutlinedTextField(
                value = label,
                onValueChange = { label = it.take(120) },
                singleLine = true,
                label = { Text("Label") },
                modifier = Modifier.focusRequester(focus),
                keyboardOptions = KeyboardOptions(imeAction = ImeAction.Done),
                keyboardActions =
                    KeyboardActions(onDone = { if (label.isNotBlank()) onSave(label) }),
            )
        },
        confirmButton = {
            TextButton(onClick = { onSave(label) }, enabled = label.isNotBlank()) { Text("Save") }
        },
        dismissButton = {
            Row {
                if (event != null)
                    TextButton(onClick = onDelete) { Text("Delete", color = palette.bad) }
                TextButton(onClick = onDismiss) { Text("Cancel") }
            }
        },
    )
    LaunchedEffect(Unit) { focus.requestFocus() }
}
