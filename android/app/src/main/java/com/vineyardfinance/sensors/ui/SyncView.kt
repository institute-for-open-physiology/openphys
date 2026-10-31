package com.vineyardfinance.sensors.ui

import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.*
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.vineyardfinance.sensors.BuildConfig

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SyncView(state: MobileState) {
    var details by remember { mutableStateOf(false) }
    val context = LocalContext.current
    val hostname = Uri.parse(BuildConfig.DEFAULT_SERVER_URL).host.orEmpty()
    val status =
        when {
            !state.accountConnected -> "Not connected"
            state.syncError != null || state.status.backlogStatusError != null -> "Retrying"
            !state.status.backlogKnown -> "Checking"
            state.status.backlogRecords > 0 -> "Syncing"
            else -> "Up to date"
        }
    val color =
        when {
            !state.accountConnected -> palette.faint
            state.syncError != null || state.status.backlogStatusError != null -> palette.bad
            !state.status.backlogKnown -> palette.faint
            else -> palette.up
        }
    Page(MobileScreen.Sync.title) {
        Column {
            Row(
                Modifier.fillMaxWidth().padding(vertical = 16.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(22.dp),
            ) {
                AppIcon(Icon.Phone, Modifier.size(34.dp), palette.muted)
                Column(verticalArrangement = Arrangement.spacedBy(5.dp)) {
                    Text("This phone", fontSize = 20.sp, fontWeight = FontWeight.Medium)
                    Text("Your recordings", fontSize = 13.sp, color = palette.muted)
                }
            }
            val time = frameTime()
            val line = palette.line
            Canvas(Modifier.fillMaxWidth().height(90.dp).padding(start = 17.dp)) {
                val path =
                    Path().apply {
                        moveTo(0f, 0f)
                        lineTo(0f, size.height * .6f)
                        cubicTo(
                            0f,
                            size.height,
                            20.dp.toPx(),
                            size.height,
                            50.dp.toPx(),
                            size.height,
                        )
                    }
                drawPath(path, line, style = Stroke(1.dp.toPx()))
                if (
                    state.accountConnected &&
                        state.syncError == null &&
                        state.status.backlogRecords > 0
                ) {
                    val fraction = ((time % 2) / 2).toFloat()
                    drawCircle(color, 2.5.dp.toPx(), Offset(0f, size.height * .65f * fraction))
                }
            }
            Row(
                Modifier.fillMaxWidth()
                    .clickable { details = true }
                    .padding(start = 56.dp, top = 18.dp, bottom = 22.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(17.dp),
            ) {
                AppIcon(Icon.Server, Modifier.size(31.dp), palette.muted)
                Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text("OpenPhys pilot server", fontSize = 19.sp, fontWeight = FontWeight.Medium)
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(7.dp),
                    ) {
                        StatusDot(color, status)
                        Text(status, fontSize = 12.sp, color = palette.muted)
                    }
                }
                AppIcon(Icon.Chevron, Modifier.size(14.dp), palette.muted)
            }
        }
        if (!state.accountConnected) ActionButton("Connect account", onClick = { details = true })
    }
    if (details)
        ModalBottomSheet(onDismissRequest = { details = false }, containerColor = palette.surface) {
            Column(
                Modifier.padding(horizontal = 24.dp).padding(bottom = 36.dp),
                verticalArrangement = Arrangement.spacedBy(28.dp),
            ) {
                Text("OpenPhys pilot server", fontSize = 22.sp, fontWeight = FontWeight.SemiBold)
                Row(
                    Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    AppIcon(Icon.Phone, Modifier.size(38.dp), palette.muted)
                    Text("→", fontSize = 24.sp, color = palette.faint)
                    AppIcon(Icon.Server, Modifier.size(38.dp), palette.muted)
                }
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    StatusDot(color, status)
                    Text(status, fontSize = 17.sp)
                    Spacer(Modifier.weight(1f))
                    if (state.accountConnected)
                        Text(
                            if (state.status.backlogKnown) "${state.status.backlogRecords} left"
                            else "--",
                            fontSize = 13.sp,
                            color = palette.muted,
                        )
                }
                HorizontalDivider(color = palette.line)
                Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Text("Server", fontSize = 12.sp, color = palette.muted)
                    Text(hostname, fontSize = 14.sp)
                }
                (state.syncError ?: state.status.backlogStatusError)?.let {
                    Text(it, fontSize = 13.sp, color = palette.bad)
                }
                if (!state.accountConnected) {
                    Text(
                        "Sign in, then choose Connect this phone.",
                        fontSize = 13.sp,
                        color = palette.muted,
                    )
                    ActionButton(
                        "Connect account",
                        onClick = {
                            context.startActivity(
                                Intent(
                                    Intent.ACTION_VIEW,
                                    Uri.parse(BuildConfig.DEFAULT_SERVER_URL),
                                )
                            )
                        },
                    )
                } else
                    TextButton(
                        onClick = {
                            context.startActivity(
                                Intent(
                                    Intent.ACTION_VIEW,
                                    Uri.parse(BuildConfig.DEFAULT_SERVER_URL),
                                )
                            )
                        }
                    ) {
                        Text("Open dashboard")
                    }
            }
        }
}
