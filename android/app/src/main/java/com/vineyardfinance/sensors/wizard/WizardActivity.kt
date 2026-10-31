package com.vineyardfinance.sensors.wizard

import android.annotation.SuppressLint
import android.bluetooth.BluetoothManager
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.SystemClock
import android.provider.Settings
import android.view.View
import android.widget.Button
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.SensorsApplication
import com.vineyardfinance.sensors.collector.CollectorAdmission
import com.vineyardfinance.sensors.collector.CollectorService
import com.vineyardfinance.sensors.collector.CollectorSnapshot
import com.vineyardfinance.sensors.collector.CollectorStatus
import com.vineyardfinance.sensors.collector.H10Identity
import com.vineyardfinance.sensors.databinding.ActivityWizardBinding
import androidx.activity.result.contract.ActivityResultContracts
import com.vineyardfinance.sensors.setup.ManufacturerBackgroundPolicy
import com.vineyardfinance.sensors.setup.RequiredPermissions
import com.vineyardfinance.sensors.setup.SamsungBatteryIntents
import kotlinx.coroutines.launch

/**
 * The step-by-step onboarding wizard (docs/onboarding_wizard_proposal_20260808.md).
 * Navigation, copy and gating live in the pure wizard core; this activity is
 * the thin Android shell: per-step zones, the live sensor scan, the battery
 * buttons shared with the main screen, and the heartbeat confirmation driven
 * by the collector's status flow.
 */
class WizardActivity : AppCompatActivity() {
    private lateinit var binding: ActivityWizardBinding
    private val graph get() = (application as SensorsApplication).graph
    private var state = WizardState()
    private var sensors = listOf<DiscoveredSensor>()
    private var latestStatus = CollectorSnapshot()
    private var scanCallback: ScanCallback? = null
    private var heartbeatStartRequested = false
    private var permissionsRequested = false
    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) {
            render()
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityWizardBinding.inflate(layoutInflater)
        setContentView(binding.root)
        state = WizardState(savedInstanceState?.getInt(STATE_STEP) ?: 0)

        val policy = ManufacturerBackgroundPolicy.forManufacturer(Build.MANUFACTURER)
        binding.wizardManufacturerSettings.visibility =
            if (policy.samsungDeviceCare) View.VISIBLE else View.GONE
        binding.wizardSleepingApps.visibility =
            if (policy.samsungDeviceCare) View.VISIBLE else View.GONE
        binding.wizardManufacturerHelp.visibility =
            if (policy.required && policy.helpUrl != null) View.VISIBLE else View.GONE
        binding.wizardManufacturerConfirm.visibility =
            if (policy.required) View.VISIBLE else View.GONE
        binding.wizardManufacturerConfirm.text =
            "I completed the ${policy.label} background steps"
        binding.wizardManufacturerConfirm.isChecked =
            graph.settings.manufacturerBackgroundConfirmed(Build.MANUFACTURER)

        binding.wizardBack.setOnClickListener { move(state.back()) }
        binding.wizardNext.setOnClickListener {
            if (state.isLast) {
                finish()
            } else {
                move(state.next(currentGate()))
            }
        }
        binding.wizardUseTypedId.setOnClickListener {
            selectSensor(binding.wizardManualId.text.toString())
        }
        binding.wizardBattery.setOnClickListener {
            runCatching {
                startActivity(
                    Intent(
                        Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                        Uri.parse("package:$packageName"),
                    ),
                )
            }.onFailure {
                runCatching {
                    startActivity(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
                }
            }
        }
        binding.wizardDataUsage.setOnClickListener {
            runCatching {
                startActivity(
                    Intent(
                        Settings.ACTION_IGNORE_BACKGROUND_DATA_RESTRICTIONS_SETTINGS,
                        Uri.parse("package:$packageName"),
                    ),
                )
            }.onFailure {
                runCatching {
                    startActivity(
                        Intent(
                            Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                            Uri.parse("package:$packageName"),
                        ),
                    )
                }.onFailure {
                    binding.wizardBatteryStatus.text =
                        "Couldn't open settings on this phone. Open Android Settings → Apps → " +
                            "Training Sensors → Mobile data, and allow unrestricted data usage."
                }
            }
        }
        binding.wizardManufacturerSettings.setOnClickListener {
            runCatching {
                startActivity(
                    Intent(
                        Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                        Uri.parse("package:$packageName"),
                    ),
                )
            }
        }
        binding.wizardSleepingApps.setOnClickListener {
            val launched = SamsungBatteryIntents.launchFirstAvailable(
                SamsungBatteryIntents.candidates(),
                { candidate -> candidate.resolveActivity(packageManager) != null },
                { candidate -> runCatching { startActivity(candidate) }.isSuccess },
            )
            if (!launched) {
                runCatching {
                    startActivity(
                        Intent(
                            Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                            Uri.parse("package:$packageName"),
                        ),
                    )
                }
            }
        }
        binding.wizardManufacturerHelp.setOnClickListener {
            policy.helpUrl?.let { url ->
                runCatching { startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url))) }
            }
        }
        binding.wizardManufacturerConfirm.setOnCheckedChangeListener { _, checked ->
            graph.settings.setManufacturerBackgroundConfirmed(Build.MANUFACTURER, checked)
            render()
        }
        binding.wizardOpenDashboard.setOnClickListener {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(BuildConfig.DEFAULT_SERVER_URL)))
        }

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                CollectorStatus.updates.collect { status ->
                    latestStatus = status
                    if (state.step == WizardStep.SEE_HEARTBEAT) render()
                }
            }
        }
        render()
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        outState.putInt(STATE_STEP, state.stepIndex)
    }

    override fun onResume() {
        super.onResume()
        render()
    }

    override fun onPause() {
        super.onPause()
        stopScan()
    }

    private fun move(next: WizardState) {
        if (next == state) {
            render()
            return
        }
        stopScan()
        state = next
        render()
    }

    private fun currentGate(): WizardGate {
        val report = graph.setup.inspect(latestStatus, allowDebugCredential = BuildConfig.DEBUG)
        val battery = report.items.firstOrNull { it.name == "Background battery operation" }
        val manufacturer = report.items.firstOrNull { it.name == "Manufacturer background policy" }
        return WizardGate(
            sensorTargetValid = runCatching {
                H10Identity.configuredSerial(graph.settings.target)
            }.isSuccess,
            batterySetupComplete = battery?.complete == true && manufacturer?.complete == true,
            heartbeatConfirmed = heartbeatReceiving() && dashboardDelivering(),
        )
    }

    private fun heartbeatReceiving(): Boolean =
        latestStatus.connected && fresh(latestStatus.hrUnixNs, STREAM_FRESH_MS * 1_000_000)

    private fun dashboardDelivering(): Boolean {
        val ack = latestStatus.lastAckUnixMs ?: return false
        return System.currentTimeMillis() - ack <= ACK_FRESH_MS
    }

    private fun fresh(unixNs: Long?, windowNs: Long): Boolean {
        if (unixNs == null) return false
        return System.currentTimeMillis() * 1_000_000 - unixNs <= windowNs
    }

    private fun render() {
        val gate = currentGate()
        binding.wizardProgress.text = state.progressLabel
        binding.wizardTitle.text = state.step.title
        binding.wizardBody.text = state.step.body
        binding.wizardBack.visibility = if (state.isFirst) View.INVISIBLE else View.VISIBLE
        binding.wizardNext.text = if (state.isLast) "Finish" else "Next"
        binding.wizardNext.isEnabled = state.isLast || gate.allowsLeaving(state.step)
        binding.wizardBlocking.text = gate.blockingDetail(state.step).orEmpty()

        binding.wizardSensorZone.visibility =
            if (state.step == WizardStep.FIND_SENSOR) View.VISIBLE else View.GONE
        binding.wizardBatteryZone.visibility =
            if (state.step == WizardStep.KEEP_ALIVE) View.VISIBLE else View.GONE
        binding.wizardHeartbeatZone.visibility =
            if (state.step == WizardStep.SEE_HEARTBEAT) View.VISIBLE else View.GONE
        binding.wizardDoneZone.visibility =
            if (state.step == WizardStep.DONE) View.VISIBLE else View.GONE

        when (state.step) {
            WizardStep.FIND_SENSOR -> {
                val missing = RequiredPermissions.missing(this)
                if (missing.isEmpty()) {
                    startScan()
                    renderSensorList()
                } else {
                    // Never claim to be scanning when the platform will not
                    // let us: ask once, then explain honestly.
                    stopScan()
                    binding.wizardSensorList.removeAllViews()
                    if (!permissionsRequested) {
                        permissionsRequested = true
                        permissionLauncher.launch(missing.toTypedArray())
                    }
                    binding.wizardScanStatus.text =
                        "Allow Nearby devices access so the app can find your sensor. " +
                        "If no dialog appeared, grant it from the main screen's " +
                        "\"Grant Nearby devices permissions\" button, then return here. " +
                        "You can also type the ID printed on the sensor below."
                }
            }
            WizardStep.KEEP_ALIVE -> {
                binding.wizardBatteryStatus.text = when {
                    dataSaverRestricting() ->
                        "Data Saver is limiting this app — tap \"Allow unrestricted " +
                            "data usage\" so overnight uploads keep flowing."
                    gate.batterySetupComplete ->
                        "Battery setup complete — recording survives in your pocket."
                    else ->
                        "Complete each button above; this screen updates as you return."
                }
            }
            WizardStep.SEE_HEARTBEAT -> {
                val report =
                    graph.setup.inspect(latestStatus, allowDebugCredential = BuildConfig.DEBUG)
                val blocked = !latestStatus.serviceRunning && !report.preStartComplete
                ensureCollectionStarted()
                binding.wizardHeartRate.text = if (heartbeatReceiving()) "♥" else "…"
                binding.wizardHeartbeatStatus.text = if (blocked) {
                    // Honest blocker instead of blaming the strap: recording
                    // has not started because a prerequisite is incomplete.
                    "Recording has not started yet: " +
                        (report.firstIncomplete?.detail ?: "review the checklist on the main screen")
                } else {
                    listOf(
                        if (heartbeatReceiving()) {
                            "Receiving your heartbeat ✓"
                        } else {
                            "Waiting for your heartbeat… (strap on, pads wet?)"
                        },
                        if (dashboardDelivering()) {
                            "Sending it to your dashboard ✓"
                        } else {
                            "Waiting for the first upload confirmation…"
                        },
                    ).joinToString("\n")
                }
            }
            else -> Unit
        }
    }

    private fun dataSaverRestricting(): Boolean = runCatching {
        getSystemService(android.net.ConnectivityManager::class.java).restrictBackgroundStatus ==
            android.net.ConnectivityManager.RESTRICT_BACKGROUND_STATUS_ENABLED
    }.getOrDefault(false)

    private fun ensureCollectionStarted() {
        if (heartbeatStartRequested || latestStatus.serviceRunning) return
        val report = graph.setup.inspect(latestStatus, allowDebugCredential = BuildConfig.DEBUG)
        if (CollectorAdmission.canStartFromUi(latestStatus.serviceRunning, report)) {
            heartbeatStartRequested = true
            runCatching { CollectorService.start(this) }
        }
    }

    private fun selectSensor(rawId: String) {
        runCatching { graph.settings.target = rawId }
            .onSuccess {
                binding.wizardManualId.setText(graph.settings.target)
                render()
            }
            .onFailure { error ->
                binding.wizardBlocking.text =
                    error.message ?: "Enter the ID printed on the sensor"
            }
    }

    private fun renderSensorList() {
        val host = binding.wizardSensorList
        host.removeAllViews()
        val selected = runCatching {
            H10Identity.configuredSerial(graph.settings.target)
        }.getOrNull()
        for (sensor in sensors) {
            val row = Button(this)
            val marker = if (sensor.serial == selected) " ✓" else ""
            row.text =
                "Polar H10 ${sensor.serial} — ${SensorScanList.signalLabel(sensor.rssi)}$marker"
            row.setOnClickListener { selectSensor(sensor.serial) }
            host.addView(row)
        }
        binding.wizardScanStatus.text = when {
            sensors.isNotEmpty() -> "Tap your sensor above."
            else -> "Scanning… wear the strap with wet pads to wake the sensor."
        }
    }

    @SuppressLint("MissingPermission")
    private fun startScan() {
        if (scanCallback != null) return
        val adapter =
            getSystemService(BluetoothManager::class.java)?.adapter ?: return
        val scanner = adapter.bluetoothLeScanner ?: return
        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                // Advertisement-local name first (present for an advertising
                // H10 and needs no CONNECT permission), device cache second —
                // the collector's proven resolution order.
                val name = result.scanRecord?.deviceName
                    ?: runCatching { result.device?.name }.getOrNull()
                sensors = SensorScanList.upsert(
                    sensors,
                    name,
                    result.rssi,
                    SystemClock.elapsedRealtime(),
                )
                if (state.step == WizardStep.FIND_SENSOR) renderSensorList()
            }
        }
        // Unfiltered scan is deliberate: the wizard runs foreground with the
        // screen on, and prefix name filters are not supported by ScanFilter.
        val started = runCatching {
            scanner.startScan(
                emptyList(),
                ScanSettings.Builder().setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY).build(),
                callback,
            )
        }.isSuccess
        if (started) scanCallback = callback
    }

    @SuppressLint("MissingPermission")
    private fun stopScan() {
        val callback = scanCallback ?: return
        scanCallback = null
        val adapter = getSystemService(BluetoothManager::class.java)?.adapter ?: return
        runCatching { adapter.bluetoothLeScanner?.stopScan(callback) }
        sensors = emptyList()
    }

    companion object {
        private const val STATE_STEP = "wizard_step"
        private const val STREAM_FRESH_MS = 8_000L
        private const val ACK_FRESH_MS = 30_000L
    }
}
