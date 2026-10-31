package com.vineyardfinance.sensors.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp

@Composable
fun MobileApp(
    state: MobileState,
    model: MobileModel?,
    connect: () -> Unit = {},
    disconnect: () -> Unit = {},
    setup: () -> Unit = {},
    initialScreen: MobileScreen = MobileScreen.Now,
) {
    var screen by rememberSaveable { mutableStateOf(initialScreen) }
    Scaffold(
        containerColor = palette.canvas,
        contentColor = palette.ink,
        bottomBar = {
            NavigationBar(containerColor = palette.canvas, tonalElevation = 0.dp) {
                MobileScreen.entries.forEach { tab ->
                    val icon =
                        when (tab) {
                            MobileScreen.Now -> Icon.Heart
                            MobileScreen.History -> Icon.History
                            MobileScreen.Device -> Icon.Device
                            MobileScreen.Sync -> Icon.Cloud
                        }
                    NavigationBarItem(
                        selected = screen == tab,
                        onClick = { screen = tab },
                        icon = {
                            AppIcon(
                                icon,
                                Modifier.size(25.dp),
                                if (screen == tab) palette.ink else palette.muted,
                            )
                        },
                        label = { Text(tab.title) },
                        colors =
                            NavigationBarItemDefaults.colors(
                                indicatorColor = Color.Transparent,
                                selectedTextColor = palette.ink,
                                unselectedTextColor = palette.muted,
                            ),
                    )
                }
            }
        },
    ) { padding ->
        Box(Modifier.padding(padding)) {
            when (screen) {
                MobileScreen.Now ->
                    NowView(state, model, openDevice = { screen = MobileScreen.Device })
                MobileScreen.History -> HistoryView(state, model)
                MobileScreen.Device -> DeviceView(state, connect, disconnect, setup)
                MobileScreen.Sync -> SyncView(state)
            }
        }
    }
    if (state.error != null)
        AlertDialog(
            onDismissRequest = { model?.dismissError() },
            text = { Text(state.error) },
            confirmButton = { TextButton(onClick = { model?.dismissError() }) { Text("OK") } },
        )
}
