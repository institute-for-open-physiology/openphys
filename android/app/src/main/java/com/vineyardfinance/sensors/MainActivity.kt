package com.vineyardfinance.sensors

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.runtime.SideEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.graphics.luminance
import androidx.compose.ui.graphics.toArgb
import androidx.core.view.WindowCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.collector.CollectorService
import com.vineyardfinance.sensors.collector.CollectorStatus
import com.vineyardfinance.sensors.ui.*
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
    private val graph
        get() = (application as SensorsApplication).graph

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        forwardEnrollment(intent)
        val model = MobileModel(graph, lifecycleScope)
        lifecycleScope.launch { repeatOnLifecycle(Lifecycle.State.STARTED) { model.observe() } }
        setContent {
            val state by model.state.collectAsStateWithLifecycle()
            MobileTheme {
                val colors = palette
                SideEffect {
                    window.statusBarColor = colors.canvas.toArgb()
                    window.navigationBarColor = colors.canvas.toArgb()
                    WindowCompat.getInsetsController(window, window.decorView).apply {
                        isAppearanceLightStatusBars = colors.canvas.luminance() > .5f
                        isAppearanceLightNavigationBars = colors.canvas.luminance() > .5f
                    }
                }
                MobileApp(
                    state,
                    model,
                    connect = ::connect,
                    disconnect = { CollectorService.stop(this) },
                    setup = { startActivity(Intent(this, SetupActivity::class.java)) },
                )
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        forwardEnrollment(intent)
    }

    private fun forwardEnrollment(source: Intent) {
        if (source.data != null) {
            startActivity(Intent(source).setClass(this, SetupActivity::class.java))
            source.data = null
        }
    }

    private fun connect() {
        val report =
            graph.setup.inspect(
                CollectorStatus.updates.value,
                allowDebugCredential = BuildConfig.DEBUG,
            )
        if (!report.preStartComplete) startActivity(Intent(this, SetupActivity::class.java))
        else {
            graph.settings.collectionEnabled = true
            CollectorService.start(this)
        }
    }
}
