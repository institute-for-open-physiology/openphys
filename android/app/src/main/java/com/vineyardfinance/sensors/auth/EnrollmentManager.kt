package com.vineyardfinance.sensors.auth

import com.vineyardfinance.sensors.network.HttpJson
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

sealed interface EnrollmentState {
    data object Idle : EnrollmentState
    data object Exchanging : EnrollmentState

    /** A one-shot event emitted only after a real enrollment exchange has persisted its credential. */
    class Succeeded internal constructor() : EnrollmentState {
        override fun toString(): String = "Succeeded"
    }

    data class Failed(val message: String, val canRetry: Boolean) : EnrollmentState
}

/** Owns fragment-token state outside the Activity and never exposes it through observable UI state. */
class EnrollmentManager(
    private val client: EnrollmentExchange,
    private val scope: CoroutineScope,
    private val onConnected: (EnrollmentResult) -> Unit = {},
    private val newExchangeId: () -> String = { UUID.randomUUID().toString() },
    private val passwordEnrollment: PasswordEnrollment? = null,
) {
    private data class Pending(val token: String, val exchangeId: String)

    private val mutex = Mutex()
    private val signInQueued = AtomicBoolean(false)
    private var pending: Pending? = null
    private var lastTerminalTokenHash: ByteArray? = null
    private val mutableState = MutableStateFlow<EnrollmentState>(EnrollmentState.Idle)
    val state: StateFlow<EnrollmentState> = mutableState.asStateFlow()

    fun submit(token: String) {
        scope.launch { exchange(Pending(token, newExchangeId())) }
    }

    fun signIn(email: String, password: String) {
        if (!signInQueued.compareAndSet(false, true)) return
        scope.launch {
            try {
                mutex.withLock {
                    pending = null
                    mutableState.value = EnrollmentState.Exchanging
                    val token = try {
                        checkNotNull(passwordEnrollment).createToken(email, password)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (error: Exception) {
                        val message = when {
                            error is HttpJson.HttpStatusException && error.status == 401 -> "Email or password is incorrect."
                            error is HttpJson.HttpStatusException && error.status == 429 -> "Too many attempts. Wait a few minutes and sign in again."
                            error is HttpJson.HttpStatusException -> "Sign-in could not complete. Please try again."
                            error is IOException -> "Could not reach the server. Check your connection and sign in again."
                            else -> "Sign-in could not complete. Please try again."
                        }
                        mutableState.value = EnrollmentState.Failed(message, canRetry = false)
                        return@withLock
                    }
                    exchangeLocked(Pending(token, newExchangeId()))
                }
            } finally { signInQueued.set(false) }
        }
    }

    fun retry() {
        scope.launch {
            mutex.withLock { pending?.let { exchangeLocked(it) } }
        }
    }

    /** Eligibility checks and credential removal are serialized with enrollment. */
    suspend fun logOut(disconnect: suspend () -> Unit) = mutex.withLock {
        disconnect()
        pending = null
        lastTerminalTokenHash = null
        mutableState.value = EnrollmentState.Idle
    }

    /** Clears only the exact success event rendered by the caller, never a newer state. */
    fun acknowledgeSuccess(success: EnrollmentState.Succeeded) {
        mutableState.compareAndSet(success, EnrollmentState.Idle)
    }

    private suspend fun exchange(candidate: Pending) = mutex.withLock { exchangeLocked(candidate) }

    private suspend fun exchangeLocked(candidate: Pending) {
        val candidateHash = MessageDigest.getInstance("SHA-256").digest(candidate.token.toByteArray(Charsets.UTF_8))
        if (lastTerminalTokenHash?.let { MessageDigest.isEqual(it, candidateHash) } == true) return
        val attempt = pending?.takeIf { it.token == candidate.token } ?: candidate.also { pending = it }
        mutableState.value = EnrollmentState.Exchanging
        try {
            val result = client.exchange(attempt.token, attempt.exchangeId)
            pending = null
            lastTerminalTokenHash = candidateHash
            mutableState.value = EnrollmentState.Succeeded()
            // Credential persistence has succeeded. Operational recovery is best-effort and must
            // never turn a completed one-use enrollment back into an authentication failure.
            runCatching { onConnected(result) }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            val terminal = error is HttpJson.HttpStatusException &&
                error.machineCode == ENROLLMENT_LINK_TERMINAL_CODE
            if (terminal) {
                pending = null
                lastTerminalTokenHash = candidateHash
            }
            mutableState.value = EnrollmentState.Failed(safeError(error, terminal), canRetry = !terminal)
        }
    }

    private fun safeError(error: Exception, terminal: Boolean): String = when {
        terminal -> {
            "Connection link is invalid, expired, or already used; generate a new link"
        }
        error is HttpJson.HttpStatusException -> "Server could not complete connection; retry the same link"
        error is IOException -> "Network unavailable; retry when connected"
        else -> error.message?.take(160) ?: "Connection failed"
    }

    companion object {
        const val ENROLLMENT_LINK_TERMINAL_CODE = "enrollment_link_terminal"
    }
}
