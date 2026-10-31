package com.vineyardfinance.sensors

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.SystemClock
import android.provider.Settings
import android.view.View
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.auth.EnrollmentState
import com.vineyardfinance.sensors.auth.EnrollmentSuccessNotice
import com.vineyardfinance.sensors.auth.EnrollmentSuccessPresentation
import com.vineyardfinance.sensors.collector.CollectorAdmission
import com.vineyardfinance.sensors.collector.CollectorService
import com.vineyardfinance.sensors.collector.CollectorSnapshot
import com.vineyardfinance.sensors.collector.CollectorStatus
import com.vineyardfinance.sensors.collector.backlogDescription
import com.vineyardfinance.sensors.collector.H10Identity
import com.vineyardfinance.sensors.databinding.ActivitySetupBinding
import androidx.core.content.FileProvider
import com.vineyardfinance.sensors.setup.ManufacturerBackgroundPolicy
import com.vineyardfinance.sensors.setup.RequiredPermissions
import com.vineyardfinance.sensors.setup.SamsungBatteryIntents
import com.vineyardfinance.sensors.update.UpdateDecision
import com.vineyardfinance.sensors.wizard.WizardActivity
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import java.io.File

class SetupActivity : AppCompatActivity() {
    private lateinit var binding: ActivitySetupBinding
    private val graph get() = (application as SensorsApplication).graph
    private var latestStatus = CollectorSnapshot()
    private var startAfterPermissions = false
    private var suppressLocationConsentListener = false
    private lateinit var enrollmentSuccessNotice: EnrollmentSuccessNotice
    private lateinit var enrollmentSuccessBanner: EnrollmentSuccessBannerUi
    private var enrollmentSuccessExpiry: Runnable? = null

    private val permissions = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { result ->
        val granted = requiredPermissions().all {
            result[it] == true || ContextCompat.checkSelfPermission(this, it) == PackageManager.PERMISSION_GRANTED
        }
        if (granted && startAfterPermissions) requestStart()
        if (!granted) binding.enrollmentStatus.text = "Setup incomplete: grant Nearby devices and notification permissions."
        startAfterPermissions = false
        renderSetup()
    }

    private val settingsActivity = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) {
        renderSetup()
    }

    // Location permission is requested only here, when the user explicitly
    // turns on the location opt-in; consent is persisted only after Android
    // grants it (docs/agreed_app_changes_20260808.md item 9).
    private val locationPermissions = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { result ->
        val granted = RequiredPermissions.location().all {
            result[it] == true || ContextCompat.checkSelfPermission(this, it) == PackageManager.PERMISSION_GRANTED
        }
        persistLocationConsent(granted)
        if (granted) requestBackgroundLocationIfMissing()
        renderLocationConsent()
        if (!granted) {
            binding.locationStatus.text =
                "GPS stays off: the location permission was not granted. Nothing was recorded."
        }
    }

    private val backgroundLocationPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) {
        renderLocationConsent()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val enrollmentLink = EnrollmentIntent.consume(intent)
        binding = ActivitySetupBinding.inflate(layoutInflater)
        setContentView(binding.root)
        enrollmentSuccessNotice = EnrollmentSuccessNotice(SystemClock::elapsedRealtime)
        enrollmentSuccessNotice.restore(
            savedInstanceState
                ?.takeIf { it.containsKey(STATE_ENROLLMENT_SUCCESS_DEADLINE) }
                ?.getLong(STATE_ENROLLMENT_SUCCESS_DEADLINE),
        )
        enrollmentSuccessBanner = EnrollmentSuccessBannerUi(binding.root, binding.enrollmentSuccess)
        renderEnrollmentSuccess(enrollmentSuccessNotice.current())
        binding.target.setText(graph.settings.target)
        binding.installation.text = "Installation ID: ${graph.identity.installationId}\nServer: ${BuildConfig.DEFAULT_SERVER_URL}"
        val backgroundPolicy = ManufacturerBackgroundPolicy.forManufacturer(Build.MANUFACTURER)
        binding.manufacturerConfirm.isChecked = graph.settings.manufacturerBackgroundConfirmed(Build.MANUFACTURER)
        binding.manufacturerConfirm.visibility = if (backgroundPolicy.required) View.VISIBLE else View.GONE
        binding.manufacturerSettings.visibility = if (backgroundPolicy.samsungDeviceCare) View.VISIBLE else View.GONE
        binding.manufacturerSleepingApps.visibility = if (backgroundPolicy.samsungDeviceCare) View.VISIBLE else View.GONE
        binding.manufacturerHelp.visibility =
            if (backgroundPolicy.required && backgroundPolicy.helpUrl != null) View.VISIBLE else View.GONE
        binding.manufacturerGuidance.visibility = if (backgroundPolicy.required) View.VISIBLE else View.GONE
        if (backgroundPolicy.required && !backgroundPolicy.samsungDeviceCare) {
            // Non-Samsung vendors have no reliable settings deep link: show
            // the manual path and the vendor instructions link instead.
            binding.manufacturerGuidance.text = backgroundPolicy.incompleteDetail
            binding.manufacturerConfirm.text =
                "I completed the ${backgroundPolicy.label} background steps"
        }

        binding.openWizard.setOnClickListener {
            startActivity(Intent(this, WizardActivity::class.java))
        }
        binding.secaEcg.setOnClickListener {
            startActivity(Intent(this, com.vineyardfinance.sensors.seca.SecaActivity::class.java))
        }
        binding.signIn.setOnClickListener {
            startActivity(Intent(this, com.vineyardfinance.sensors.auth.SignInActivity::class.java))
        }
        binding.logOut.setOnClickListener { showLogOut() }
        binding.connectPhone.setOnClickListener {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(BuildConfig.DEFAULT_SERVER_URL)))
        }
        binding.retryEnrollment.setOnClickListener { graph.enrollment.retry() }
        binding.bluetoothSettings.setOnClickListener {
            settingsActivity.launch(Intent(Settings.ACTION_BLUETOOTH_SETTINGS))
        }
        binding.permissionSettings.setOnClickListener {
            permissions.launch(requiredPermissions().toTypedArray())
        }
        binding.notificationSettings.setOnClickListener { openNotificationSetup() }
        binding.batterySettings.setOnClickListener { openBatterySetup() }
        binding.manufacturerSettings.setOnClickListener { openApplicationSettings() }
        binding.manufacturerSleepingApps.setOnClickListener { openSamsungNeverSleepingApps() }
        binding.manufacturerHelp.setOnClickListener {
            backgroundPolicy.helpUrl?.let { url ->
                // A phone without any browser must not crash the setup guide.
                runCatching { startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url))) }
            }
        }
        binding.manufacturerConfirm.setOnCheckedChangeListener { _, checked ->
            graph.settings.setManufacturerBackgroundConfirmed(Build.MANUFACTURER, checked)
            renderSetup()
        }
        binding.locationConsent.setOnCheckedChangeListener { _, checked ->
            if (suppressLocationConsentListener) return@setOnCheckedChangeListener
            if (checked) {
                val missing = RequiredPermissions.missingLocation(this)
                if (missing.isEmpty()) {
                    persistLocationConsent(true)
                    requestBackgroundLocationIfMissing()
                    renderLocationConsent()
                } else {
                    locationPermissions.launch(missing.toTypedArray())
                }
            } else {
                persistLocationConsent(false)
                renderLocationConsent()
            }
        }
        binding.locationBackgroundSettings.setOnClickListener { requestBackgroundLocationIfMissing() }
        renderLocationConsent()
        binding.start.setOnClickListener { requestStart() }
        binding.stop.setOnClickListener {
            if (latestStatus.serviceRunning) {
                CollectorService.stop(this)
            } else {
                runCatching { graph.settings.recordCollectorStopped() }.onFailure { error ->
                    binding.enrollmentStatus.text = error.message ?: "Could not persist the collection stop"
                }
            }
            renderSetup()
        }
        binding.dashboard.setOnClickListener {
            // Device credentials are intentionally never attached to browser/website requests.
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(BuildConfig.DEFAULT_SERVER_URL)))
        }

        handleEnrollmentLink(enrollmentLink)
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                launch {
                    CollectorStatus.updates.collect { status ->
                        latestStatus = status
                        renderCollector(status)
                        renderSetup()
                    }
                }
                launch {
                    graph.enrollment.state.collect { renderEnrollment(it) }
                }
                launch {
                    graph.credentials.connected.collect { renderSetup() }
                }
            }
        }
        if (BuildConfig.DEBUG && intent.getBooleanExtra(EXTRA_LOCAL_SMOKE, false)) requestStart()
    }

    override fun onResume() {
        super.onResume()
        renderSetup()
        renderLocationConsent()
    }

    override fun onSaveInstanceState(outState: Bundle) {
        enrollmentSuccessNotice.savedDeadline()?.let { deadline ->
            outState.putLong(STATE_ENROLLMENT_SUCCESS_DEADLINE, deadline)
        }
        super.onSaveInstanceState(outState)
    }

    override fun onDestroy() {
        enrollmentSuccessExpiry?.let(binding.enrollmentSuccess::removeCallbacks)
        enrollmentSuccessExpiry = null
        enrollmentSuccessBanner.cancelPendingAttention()
        super.onDestroy()
    }

    override fun onNewIntent(intent: Intent) {
        val enrollmentLink = EnrollmentIntent.consume(intent)
        super.onNewIntent(intent)
        setIntent(intent)
        handleEnrollmentLink(enrollmentLink)
    }

    private fun handleEnrollmentLink(link: ConsumedEnrollmentLink) {
        when (link) {
            ConsumedEnrollmentLink.Absent -> Unit
            ConsumedEnrollmentLink.Invalid -> {
                renderEnrollmentSuccess(enrollmentSuccessNotice.present(EnrollmentState.Exchanging))
                binding.enrollmentStatus.text =
                    "Connection link is invalid. Generate a fresh link on the signed-in website."
            }
            is ConsumedEnrollmentLink.Valid -> {
                if (graph.credentials.connected.value) {
                    // Already connected: never re-exchange (that would only
                    // rotate the device credential) — treat the scan as an
                    // update check instead.
                    beginScannedUpdateCheck(link.token)
                } else {
                    renderEnrollmentSuccess(enrollmentSuccessNotice.present(EnrollmentState.Exchanging))
                    graph.enrollment.submit(link.token)
                }
            }
        }
    }

    private fun beginScannedUpdateCheck(token: String) {
        binding.updateApp.visibility = View.GONE
        binding.enrollmentStatus.text = "This phone is already connected. Checking for app updates…"
        lifecycleScope.launch {
            val preview = try {
                graph.updates.preview(token)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                null
            }
            when (
                val action = UpdateDecision.forScannedLink(
                    enrolled = graph.credentials.connected.value,
                    installedVersionCode = BuildConfig.VERSION_CODE.toLong(),
                    serverPreview = preview,
                )
            ) {
                UpdateDecision.Action.ProceedWithConnection -> {
                    renderEnrollmentSuccess(enrollmentSuccessNotice.present(EnrollmentState.Exchanging))
                    graph.enrollment.submit(token)
                }
                UpdateDecision.Action.AlreadyUpToDate -> {
                    binding.enrollmentStatus.text =
                        "This phone is already connected and the app (${BuildConfig.VERSION_NAME}) is up to date."
                }
                UpdateDecision.Action.CheckUnavailable -> {
                    binding.enrollmentStatus.text =
                        "This phone is already connected. The update check did not complete — " +
                        "if the link expired, regenerate the QR code and scan again."
                }
                is UpdateDecision.Action.UpdateAvailable -> {
                    val ticket = preview?.ticket
                    if (ticket == null) {
                        binding.enrollmentStatus.text =
                            "Update ${action.versionName} is available, but the download could not be " +
                            "authorized — regenerate the QR code and scan again."
                        return@launch
                    }
                    binding.enrollmentStatus.text =
                        "App update available: ${action.versionName} (you have ${BuildConfig.VERSION_NAME})."
                    binding.updateApp.visibility = View.VISIBLE
                    binding.updateApp.setOnClickListener {
                        downloadAndInstallUpdate(ticket, preview.sha256, action.versionName)
                    }
                }
            }
        }
    }

    private fun downloadAndInstallUpdate(ticket: String, expectedSha256: String, versionName: String) {
        binding.updateApp.isEnabled = false
        binding.enrollmentStatus.text = "Downloading update $versionName…"
        lifecycleScope.launch {
            val downloaded = try {
                Result.success(
                    graph.updates.downloadVerified(
                        ticket,
                        expectedSha256,
                        File(cacheDir, "updates/training-sensors.apk"),
                    ),
                )
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                Result.failure(error)
            }
            binding.updateApp.isEnabled = true
            downloaded.fold(
                onSuccess = { file ->
                    binding.enrollmentStatus.text =
                        "Update verified. Follow the Android installer to finish — collection " +
                        "settings and the connection are preserved."
                    val uri = FileProvider.getUriForFile(
                        this@SetupActivity,
                        "$packageName.fileprovider",
                        file,
                    )
                    val install = Intent(Intent.ACTION_VIEW)
                        .setDataAndType(uri, "application/vnd.android.package-archive")
                        .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    runCatching { startActivity(install) }.onFailure {
                        binding.enrollmentStatus.text =
                            "Could not open the Android installer. Open the downloaded file manually " +
                            "or update from the signed-in website."
                    }
                },
                onFailure = { error ->
                    binding.enrollmentStatus.text = when {
                        error.message?.contains("HTTP 401") == true ->
                            "The update link expired. Regenerate the QR code and scan again."
                        error.message?.contains("integrity") == true ->
                            "The downloaded update failed verification and was discarded. Try again."
                        else -> "Update download failed — check the connection and try again."
                    }
                },
            )
        }
    }

    private fun showLogOut() {
        androidx.appcompat.app.AlertDialog.Builder(this)
            .setTitle("Log out of this phone?")
            .setMessage("Stop recording first. Log out once all recordings have uploaded, then sign back in with the same account. To use a different account, choose Discard local data. Uploaded recordings remain on the server.")
            .setPositiveButton("Log out") { _, _ ->
                binding.logOut.isEnabled = false
                lifecycleScope.launch {
                    try {
                        graph.enrollment.logOut {
                            graph.logout.logOut()
                        }
                        renderEnrollment(EnrollmentState.Idle)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (error: Exception) {
                        binding.enrollmentStatus.text = error.message ?: "Could not log out. Please try again."
                    } finally {
                        binding.logOut.isEnabled = true
                    }
                }
            }
            .setNeutralButton("Discard local data…") { _, _ -> confirmDiscardAndLogOut() }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun confirmDiscardAndLogOut() {
        androidx.appcompat.app.AlertDialog.Builder(this)
            .setTitle("Discard local data and log out?")
            .setMessage("This permanently deletes recordings that have not uploaded and resets this app's local settings. Recording stops and the app closes. Uploaded recordings remain on the server.")
            .setPositiveButton("Discard and log out") { _, _ ->
                // Android stops this process and its workers before atomically clearing app data,
                // including both queues, identity and credential. No writer can race manual deletion.
                val manager = getSystemService(android.app.ActivityManager::class.java)
                if (!manager.clearApplicationUserData()) {
                    binding.enrollmentStatus.text = "Could not reset local app data. Try again."
                }
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun renderEnrollment(state: EnrollmentState) {
        val successPresentation = enrollmentSuccessNotice.present(state)
        renderEnrollmentSuccess(successPresentation)
        binding.retryEnrollment.visibility = View.GONE
        binding.enrollmentStatus.text = when (state) {
            EnrollmentState.Idle -> if (graph.credentials.connected.value) {
                "This phone is connected for background upload."
            } else {
                "Sign in with your email and password below, or connect through the website."
            }
            EnrollmentState.Exchanging -> "Connecting this installation…"
            is EnrollmentState.Succeeded -> "Connection complete. Continue setup below."
            is EnrollmentState.Failed -> {
                binding.retryEnrollment.visibility = if (state.canRetry) View.VISIBLE else View.GONE
                state.message
            }
        }
        successPresentation.successToAcknowledge?.let(graph.enrollment::acknowledgeSuccess)
        renderSetup()
    }

    private fun renderEnrollmentSuccess(presentation: EnrollmentSuccessPresentation) {
        enrollmentSuccessExpiry?.let(binding.enrollmentSuccess::removeCallbacks)
        enrollmentSuccessExpiry = null
        enrollmentSuccessBanner.render(presentation)
        val remainingMillis = presentation.remainingMillis ?: return
        lateinit var expiry: Runnable
        expiry = Runnable {
            if (enrollmentSuccessExpiry !== expiry) return@Runnable
            enrollmentSuccessExpiry = null
            renderEnrollmentSuccess(enrollmentSuccessNotice.current())
        }
        enrollmentSuccessExpiry = expiry
        binding.enrollmentSuccess.postDelayed(expiry, remainingMillis)
    }

    private fun requestStart() {
        val configuredTarget = runCatching {
            H10Identity.configuredSerial(binding.target.text.toString())
        }.getOrElse {
            binding.enrollmentStatus.text = it.message ?: "Enter the H10 serial shown on the sensor"
            return
        }
        graph.settings.target = configuredTarget
        val missing = requiredPermissions().filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }
        if (missing.isNotEmpty()) {
            startAfterPermissions = true
            permissions.launch(missing.toTypedArray())
            return
        }
        val report = graph.setup.inspect(latestStatus, allowDebugCredential = BuildConfig.DEBUG)
        if (!report.preStartComplete) {
            binding.enrollmentStatus.text = "Setup incomplete: ${report.firstIncomplete?.detail ?: "review the checklist"}"
            renderSetup()
            return
        }
        graph.settings.collectionEnabled = true
        CollectorService.start(this)
        renderSetup()
    }

    private fun renderCollector(status: CollectorSnapshot) {
        binding.state.text = "${status.state}\n${status.detail}"
        binding.telemetry.text = buildString {
            append("Connected: ${status.connected}\n")
            append("RSSI: ${status.rssiDbm?.let { "$it dBm" } ?: "—"}  Battery: ${status.batteryPercent?.let { "$it%" } ?: "—"}\n")
            append("Freshness: ECG ${age(status.ecgUnixNs)}, ACC ${age(status.accUnixNs)}, HR ${age(status.hrUnixNs)}\n")
            append("Durable phone backlog: ${status.backlogDescription()}\n")
            append("Upload: ${status.lastAck ?: "waiting"}")
            status.lastError?.let { append("\nLast error: $it") }
            status.backlogStatusError?.let { append("\nQueue status error: $it") }
        }
    }

    private fun renderSetup() {
        if (!::binding.isInitialized) return
        binding.signIn.visibility = if (graph.credentials.connected.value) View.GONE else View.VISIBLE
        binding.logOut.visibility = if (graph.credentials.connected.value) View.VISIBLE else View.GONE
        val report = graph.setup.inspect(latestStatus, allowDebugCredential = BuildConfig.DEBUG)
        binding.setupStatus.text = when {
            report.complete -> "Setup complete"
            report.preStartComplete -> "Setup incomplete — collection/cloud health is still being established"
            else -> "Setup incomplete — complete the required steps below"
        }
        binding.checklist.text = report.items.joinToString("\n\n") {
            "${if (it.complete) "✓" else "○"} ${it.name}\n${it.detail}"
        }
        binding.start.isEnabled = CollectorAdmission.canStartFromUi(latestStatus.serviceRunning, report)
        binding.stop.isEnabled = CollectorAdmission.canStopFromUi(
            latestStatus.serviceRunning,
            graph.settings.collectionEnabled,
        )
    }

    private fun openNotificationSetup() {
        val notificationMissing = Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        if (notificationMissing) {
            permissions.launch(arrayOf(Manifest.permission.POST_NOTIFICATIONS))
        } else {
            settingsActivity.launch(
                Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS)
                    .putExtra(Settings.EXTRA_APP_PACKAGE, packageName),
            )
        }
    }

    private fun openBatterySetup() {
        val direct = Intent(
            Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
            Uri.parse("package:$packageName"),
        )
        runCatching { settingsActivity.launch(direct) }.onFailure {
            settingsActivity.launch(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
        }
    }

    private fun openApplicationSettings() {
        settingsActivity.launch(
            Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:$packageName")),
        )
    }

    private fun openSamsungNeverSleepingApps() {
        val launched = SamsungBatteryIntents.launchFirstAvailable(
            SamsungBatteryIntents.candidates(),
            { candidate -> candidate.resolveActivity(packageManager) != null },
            { candidate -> runCatching { settingsActivity.launch(candidate) }.isSuccess },
        )
        // Never dead-end the guided setup: without a resolvable Device care
        // screen, fall back to the app's own settings page; the guidance
        // copy names the manual Settings path for exactly this case.
        if (!launched) openApplicationSettings()
    }

    private fun requiredPermissions(): List<String> = RequiredPermissions.list()

    private fun persistLocationConsent(enabled: Boolean) {
        runCatching { graph.settings.locationCollectionEnabled = enabled }.onFailure { error ->
            binding.locationStatus.text = error.message ?: "Could not persist the location setting"
        }
    }

    private fun requestBackgroundLocationIfMissing() {
        val permission = RequiredPermissions.backgroundLocation() ?: return
        if (ContextCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED) return
        // On Android 11+ this opens the system screen with "Allow all the time".
        backgroundLocationPermission.launch(permission)
    }

    private fun renderLocationConsent() {
        if (!::binding.isInitialized) return
        val enabled = graph.settings.locationCollectionEnabled
        suppressLocationConsentListener = true
        binding.locationConsent.isChecked = enabled
        suppressLocationConsentListener = false
        val permissionMissing = RequiredPermissions.missingLocation(this).isNotEmpty()
        val backgroundGranted = RequiredPermissions.backgroundLocationGranted(this)
        binding.locationStatus.text = when {
            !enabled -> "GPS is off. No location data is collected or leaves this phone."
            permissionMissing ->
                "GPS is on but Android's location permission is missing, so nothing is being " +
                    "recorded. Turn the toggle off and on again to grant it."
            !backgroundGranted ->
                "GPS records while collection runs. Choose \"Allow all the time\" so it keeps " +
                    "recording after a phone restart without opening the app."
            else ->
                "GPS records while collection runs: about once a second while you are moving, " +
                    "occasionally while still."
        }
        binding.locationBackgroundSettings.visibility =
            if (enabled && !permissionMissing && !backgroundGranted) View.VISIBLE else View.GONE
    }

    private fun age(unixNs: Long?): String = unixNs?.let {
        "${(System.currentTimeMillis() - it / 1_000_000L).coerceAtLeast(0)} ms"
    } ?: "—"

    companion object {
        const val EXTRA_LOCAL_SMOKE = "local_smoke"
        private const val STATE_ENROLLMENT_SUCCESS_DEADLINE = "enrollment_success_deadline"
    }
}
