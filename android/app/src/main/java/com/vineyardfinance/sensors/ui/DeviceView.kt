package com.vineyardfinance.sensors.ui

import androidx.compose.foundation.*
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

@Composable
fun DeviceView(state: MobileState, connect: () -> Unit, disconnect: () -> Unit, setup: () -> Unit) {
    val status = state.status
    Page(MobileScreen.Device.title) {
        Column(
            Modifier.fillMaxWidth(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(18.dp),
        ) {
            H10Illustration()
            Text("Polar H10", fontSize = 23.sp, fontWeight = FontWeight.SemiBold)
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(7.dp),
            ) {
                StatusDot(
                    if (status.connected) palette.up else palette.faint,
                    if (status.connected) "Connected" else "Disconnected",
                )
                Text(
                    if (status.connected) "Connected"
                    else if (status.serviceRunning) status.state else "Disconnected",
                    fontSize = 12.sp,
                    color = palette.muted,
                )
            }
            if (!status.connected)
                ActionButton(
                    if (status.serviceRunning) "Connecting…" else "Connect",
                    enabled = !status.serviceRunning,
                    onClick = connect,
                )
        }
        Column(
            Modifier.fillMaxWidth()
                .background(palette.surface, RoundedCornerShape(Layout.cardRadius.dp))
                .padding(horizontal = Layout.rowInset.dp)
        ) {
            StatusRow(
                Icon.Battery,
                "Battery",
                status.batteryPercent?.let { "$it%" } ?: "--",
                batteryColor(status.batteryPercent),
            )
            HorizontalDivider(color = palette.line)
            StatusRow(
                Icon.Signal,
                "Signal",
                status.rssiDbm?.let { "$it dBm" } ?: "--",
                signalColor(status.rssiDbm),
            )
            HorizontalDivider(color = palette.line)
            StatusRow(
                Icon.Heart,
                "Skin contact",
                state.signals.contact?.let { if (it) "Detected" else "Lost" } ?: "--",
                state.signals.contact?.let { if (it) palette.up else palette.bad } ?: palette.faint,
            )
            HorizontalDivider(color = palette.line)
            StatusRow(
                Icon.Clock,
                "Clock",
                if (!status.connected) "--"
                else if (state.signals.clockSynced) "Synced" else "Not synced",
                if (!status.connected) palette.faint
                else if (state.signals.clockSynced) palette.up else palette.warn,
            )
        }
        if (!status.connected && status.lastError != null)
            Text(status.lastError, fontSize = 13.sp, color = palette.bad)
        Column(verticalArrangement = Arrangement.spacedBy(18.dp)) {
            SectionTitle(DeviceContent.timeline)
            if (state.timeline.isEmpty())
                Text("Connection events appear here.", fontSize = 13.sp, color = palette.muted)
            state.timeline.forEachIndexed { index, event ->
                Row(horizontalArrangement = Arrangement.spacedBy(15.dp)) {
                    Text(
                        formatTime(event.time),
                        fontSize = 11.sp,
                        color = palette.muted,
                        modifier = Modifier.width(42.dp).padding(top = 2.dp),
                    )
                    Column(
                        Modifier.width(6.dp),
                        horizontalAlignment = Alignment.CenterHorizontally,
                    ) {
                        Spacer(Modifier.height(6.dp))
                        StatusDot(palette.faint, event.title)
                        if (index < state.timeline.lastIndex)
                            Box(
                                Modifier.width(1.dp)
                                    .height(40.dp)
                                    .padding(top = 6.dp)
                                    .background(palette.line)
                            )
                    }
                    Column(verticalArrangement = Arrangement.spacedBy(5.dp)) {
                        Text(
                            if (event.title == "Streaming") "Connected" else event.title,
                            fontSize = 13.sp,
                            fontWeight = FontWeight.Medium,
                        )
                        Text(event.detail, fontSize = 11.sp, color = palette.muted)
                    }
                }
            }
        }
        TextButton(onClick = setup) { Text("Device settings") }
        if (status.serviceRunning)
            ActionButton(
                if (status.connected) "Disconnect" else "Cancel connection",
                destructive = true,
                onClick = disconnect,
            )
    }
}

@Composable
private fun StatusRow(icon: Icon, label: String, value: String, color: Color) {
    Row(
        Modifier.fillMaxWidth().padding(vertical = 18.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        AppIcon(icon, Modifier.size(20.dp), palette.muted)
        Text(label, fontSize = 14.sp, color = palette.muted, modifier = Modifier.weight(1f))
        Text(value, fontSize = 14.sp)
        StatusDot(color, label)
    }
}

@Composable
private fun H10Illustration() {
    Box(Modifier.fillMaxWidth().height(122.dp), contentAlignment = Alignment.Center) {
        Box(
            Modifier.fillMaxWidth(.86f)
                .height(35.dp)
                .background(palette.raised.copy(alpha = .45f), RoundedCornerShape(100))
        )
        Box(
            Modifier.size(158.dp, 94.dp)
                .background(
                    Brush.linearGradient(listOf(Color(0xff303030), Color(0xff0e0e0e))),
                    RoundedCornerShape(100),
                )
                .border(1.dp, Color.White.copy(alpha = .08f), RoundedCornerShape(100)),
            contentAlignment = Alignment.Center,
        ) {
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(7.dp),
            ) {
                Text(
                    "POLAR",
                    fontSize = 17.sp,
                    fontWeight = FontWeight.Bold,
                    letterSpacing = 3.sp,
                    color = Color(0xff8c8c8c),
                )
                Text("H10", fontSize = 9.sp, letterSpacing = 2.sp, color = Color(0xff545454))
            }
        }
    }
}
