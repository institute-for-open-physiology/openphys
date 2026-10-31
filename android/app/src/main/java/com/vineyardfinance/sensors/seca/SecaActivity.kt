package com.vineyardfinance.sensors.seca

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothAdapter
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.util.Log
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.Spinner
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.SensorsApplication
import kotlinx.coroutines.launch
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File

/** UI/platform boundary; wire framing, durable queue and ACK logic have deterministic tests. */
class SecaActivity : AppCompatActivity() {
    private val graph get() = (application as SensorsApplication).graph
    private lateinit var devices: Spinner
    private lateinit var rates: Spinner
    private lateinit var status: TextView
    private var addresses = emptyList<String>()
    private val permissions = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { refreshDevices() }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        title = "seca 12-lead ECG"
        val layout = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(28, 28, 28, 28); setBackgroundColor(Color.rgb(15, 23, 42)) }
        fun text(value: String, size: Float = 17f) = TextView(this).apply { text = value; textSize = size; setTextColor(Color.WHITE); setPadding(0, 12, 0, 12); layout.addView(this) }
        fun button(value: String, action: () -> Unit) = Button(this).apply { text = value; setOnClickListener { action() }; layout.addView(this) }
        text("seca 12-lead ECG", 26f)
        text("Pair the seca in Bluetooth settings, then choose it below. When moving from USB, unplug and restart the recorder first.")
        button("Pair / review Bluetooth") { startActivity(Intent(Settings.ACTION_BLUETOOTH_SETTINGS)) }
        button("Grant Nearby devices permission") { permissions.launch(requiredPermissions()) }
        val manual = EditText(this).apply {
            hint = "Known recorder address (AA:BB:CC:DD:EE:FF)"
            setTextColor(Color.WHITE); setHintTextColor(Color.LTGRAY)
            inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_FLAG_CAP_CHARACTERS
            setSingleLine(); layout.addView(this)
        }
        button("Pair known recorder address") { pairAddress(manual.text.toString().trim()) }
        text("Paired recorder")
        devices = Spinner(this).also { layout.addView(it) }
        button("Refresh paired devices") { refreshDevices() }
        text("Acquisition rate")
        rates = Spinner(this).also {
            it.adapter = selectorAdapter(SecaProtocol.rates.map { rate -> "${rate / 1000} kHz" })
            it.setSelection(SecaProtocol.rates.indexOf(graph.secaSettings.rate).coerceAtLeast(0)); layout.addView(it)
        }
        button("Start seca recording") {
            try {
                check(requiredPermissions().all { ContextCompat.checkSelfPermission(this, it) == PackageManager.PERMISSION_GRANTED }) { "Grant Nearby devices permission first" }
                check(graph.credentials.currentCredential() != null) { "Connect this phone to Carolina from the main screen first" }
                val address = addresses.getOrNull(devices.selectedItemPosition) ?: error("Pair and select the seca first")
                check(!SecaStatus.updates.value.running) { "Stop the current recording before changing recorder or rate" }
                graph.secaSettings.start(address, SecaProtocol.rates[rates.selectedItemPosition])
                SecaService.start(this)
            } catch (error: Exception) { SecaStatus.update { it.copy(error = error.message) } }
        }
        button("Stop seca recording") { SecaService.stop(this) }
        button("GPS consent / permissions") { startActivity(Intent(this, com.vineyardfinance.sensors.MainActivity::class.java)) }
        button("Open Carolina ECG") { startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(BuildConfig.DEFAULT_SERVER_URL + "/admin/seca"))) }
        button("Export queued seca packets for USB") {
            lifecycleScope.launch {
                try {
                    check(!SecaStatus.updates.value.running) { "Stop seca recording before exporting queued packets" }
                    val result = withContext(Dispatchers.IO) {
                        val base = getExternalFilesDir(null) ?: error("Local export storage unavailable")
                        val folder = File(base, "seca-exports/${System.currentTimeMillis()}")
                        graph.secaQueue.exportChunks(folder) to folder.absolutePath
                    }
                    Log.i("SecaExport", "Exported ${result.first} queued chunks to ${result.second}")
                    SecaStatus.update { it.copy(state = "Exported ${result.first} queued chunks to ${result.second}", error = null) }
                } catch (error: Exception) { SecaStatus.update { it.copy(error = "Export failed: ${error.message}") } }
            }
        }
        status = text("Ready")
        text("Raw packets stay on this phone until Carolina confirms durable receipt. GPS follows the main screen's location opt-in and is saved alongside ECG. Display processing is separate from the full-rate recording. Contact detection is currently disabled.", 14f)
        setContentView(ScrollView(this).apply { addView(layout) })
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                SecaStatus.updates.collect { value ->
                    status.text = "${value.state}\n${value.location}\n${value.locationUpload?.let { "Location upload: $it\n" }.orEmpty()}${value.packets} received packets · ${value.bytes / 1024} KiB\n${value.queued} queued chunks · ${value.uploaded} uploaded\n${value.session?.let { "Session $it\n" }.orEmpty()}${value.recovery?.let { "Framing recovery: ${it.episodes} episodes · ${it.recovered} recovered · ${it.corruptBytes} corrupt bytes · ${it.missingGroups} missing groups\n" }.orEmpty()}${value.error?.let { "Last recording error: $it\n" }.orEmpty()}${value.uploadError?.let { "Upload: $it\n" }.orEmpty()}"
                }
            }
        }
    }
    override fun onResume() { super.onResume(); if (::devices.isInitialized) refreshDevices() }
    private fun selectorAdapter(labels: List<String>): ArrayAdapter<String> =
        object : ArrayAdapter<String>(this, android.R.layout.simple_spinner_dropdown_item, labels) {
            override fun getView(position: Int, convertView: View?, parent: ViewGroup): View =
                (super.getView(position, convertView, parent) as TextView).apply {
                    setTextColor(Color.WHITE)
                    setBackgroundColor(Color.rgb(15, 23, 42))
                }
            override fun getDropDownView(position: Int, convertView: View?, parent: ViewGroup): View =
                (super.getDropDownView(position, convertView, parent) as TextView).apply {
                    setTextColor(Color.BLACK)
                    setBackgroundColor(Color.WHITE)
                }
        }
    @SuppressLint("MissingPermission") private fun pairAddress(address: String) {
        try {
            check(Build.VERSION.SDK_INT < 31 || ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED) { "Grant Nearby devices permission first" }
            check(BluetoothAdapter.checkBluetoothAddress(address)) { "Enter the recorder's Bluetooth address as AA:BB:CC:DD:EE:FF" }
            val adapter = getSystemService(BluetoothManager::class.java)?.adapter ?: error("Bluetooth unavailable")
            check(adapter.isEnabled) { "Turn Bluetooth on first" }
            check(adapter.getRemoteDevice(address).createBond()) { "Pairing did not start; check recorder power and Bluetooth mode" }
            SecaStatus.update { it.copy(state = "Accept Android's pairing prompt, then refresh paired devices", error = null) }
        } catch (error: Exception) { SecaStatus.update { it.copy(error = error.message) } }
    }
    private fun requiredPermissions() = buildList {
        if (Build.VERSION.SDK_INT >= 31) add(Manifest.permission.BLUETOOTH_CONNECT)
        if (Build.VERSION.SDK_INT >= 33) add(Manifest.permission.POST_NOTIFICATIONS)
    }.toTypedArray()
    @SuppressLint("MissingPermission") private fun refreshDevices() {
        if (Build.VERSION.SDK_INT >= 31 && ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED) return
        val paired = getSystemService(BluetoothManager::class.java)?.adapter?.bondedDevices.orEmpty().sortedBy { it.name ?: it.address }
        // Keep the selector specific to ECG recorders so an H10 is never opened as RFCOMM.
        val recorders = paired.filter { (it.name ?: "").contains("seca", true) || (it.name ?: "").contains("ct 33", true) || it.address == graph.secaSettings.address }
        addresses = recorders.map { it.address }
        devices.adapter = selectorAdapter(recorders.map { "${it.name ?: "seca"} · ${it.address}" }.ifEmpty { listOf("No paired seca recorder") })
        graph.secaSettings.address?.let { address -> addresses.indexOf(address).takeIf { it >= 0 }?.let(devices::setSelection) }
    }
}
