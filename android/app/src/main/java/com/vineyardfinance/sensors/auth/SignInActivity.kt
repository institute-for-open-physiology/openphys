package com.vineyardfinance.sensors.auth

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.view.WindowManager
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.vineyardfinance.sensors.BuildConfig
import com.vineyardfinance.sensors.SensorsApplication
import com.vineyardfinance.sensors.databinding.ActivitySignInBinding
import kotlinx.coroutines.launch

class SignInActivity : AppCompatActivity() {
    private lateinit var binding: ActivitySignInBinding
    private val graph get() = (application as SensorsApplication).graph

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.addFlags(WindowManager.LayoutParams.FLAG_SECURE)
        binding = ActivitySignInBinding.inflate(layoutInflater)
        setContentView(binding.root)
        binding.signIn.setOnClickListener {
            val email = binding.email.text.toString().trim()
            val password = binding.password.text.toString()
            if (email.isBlank() || password.isEmpty()) {
                binding.status.text = "Enter your email and password."
            } else {
                binding.signIn.isEnabled = false
                binding.password.text.clear()
                graph.enrollment.signIn(email, password)
            }
        }
        binding.retry.setOnClickListener { graph.enrollment.retry() }
        binding.forgotPassword.setOnClickListener {
            runCatching {
                startActivity(Intent(Intent.ACTION_VIEW, Uri.parse("${BuildConfig.DEFAULT_SERVER_URL}/forgot-password")))
            }.onFailure { binding.status.text = "Open the website in a browser to reset your password." }
        }
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                launch { graph.credentials.connected.collect { if (it) finish() } }
                launch {
                    graph.enrollment.state.collect { state ->
                        val busy = state == EnrollmentState.Exchanging
                        binding.signIn.isEnabled = !busy
                        binding.email.isEnabled = !busy
                        binding.password.isEnabled = !busy
                        binding.retry.visibility = if (state is EnrollmentState.Failed && state.canRetry) View.VISIBLE else View.GONE
                        binding.status.text = when (state) {
                            EnrollmentState.Idle -> "Use the email and password you registered on the website."
                            EnrollmentState.Exchanging -> "Signing in and connecting this phone…"
                            is EnrollmentState.Succeeded -> "Phone connected."
                            is EnrollmentState.Failed -> state.message
                        }
                    }
                }
            }
        }
    }
}
