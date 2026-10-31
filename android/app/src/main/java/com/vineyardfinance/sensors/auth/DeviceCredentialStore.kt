package com.vineyardfinance.sensors.auth

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

interface DeviceCredentialProvider {
    val connected: StateFlow<Boolean>
    fun currentCredential(): String?
}

interface DeviceCredentialStore : DeviceCredentialProvider {
    fun storeCredential(credential: String)
    fun clearIfCurrent(rejectedCredential: String): Boolean
    fun clear()
}

/** Stores the non-expiring ingestion credential encrypted by a non-exportable Android key. */
class EncryptedDeviceCredentialStore internal constructor(
    context: Context,
    private val cipher: CredentialCipher,
    preferencesName: String,
) : DeviceCredentialStore {
    constructor(context: Context) : this(context, AndroidKeystoreCredentialCipher(KEY_ALIAS), PREFERENCES)
    internal constructor(context: Context, cipher: CredentialCipher) : this(context, cipher, PREFERENCES)
    internal constructor(context: Context, preferencesName: String) :
        this(context, AndroidKeystoreCredentialCipher("$KEY_ALIAS:$preferencesName"), preferencesName)

    internal data class EncryptedPayload(val ciphertext: ByteArray, val iv: ByteArray)

    internal interface CredentialCipher {
        fun encrypt(plaintext: ByteArray): EncryptedPayload
        fun decrypt(ciphertext: ByteArray, iv: ByteArray): ByteArray
        fun deleteKey()
    }

    private val preferences = context.getSharedPreferences(preferencesName, Context.MODE_PRIVATE)
    private val mutableConnected = MutableStateFlow(false)
    override val connected: StateFlow<Boolean> = mutableConnected.asStateFlow()

    init {
        mutableConnected.value = currentCredential() != null
    }

    @Synchronized
    override fun storeCredential(credential: String) {
        require(credential.isNotBlank()) { "device credential must not be blank" }
        val plaintext = credential.toByteArray(Charsets.UTF_8)
        val encrypted = try {
            cipher.encrypt(plaintext)
        } catch (firstFailure: Exception) {
            // An invalidated alias cannot be repaired in place. Remove only this credential's
            // encrypted state and alias, then retry once with a freshly generated key. The Room
            // queue and installation identity live in separate stores and remain untouched.
            clearCredentialPreferencesChecked()
            cipher.deleteKey()
            try {
                cipher.encrypt(plaintext)
            } catch (secondFailure: Exception) {
                secondFailure.addSuppressed(firstFailure)
                throw secondFailure
            }
        } finally {
            plaintext.fill(0)
        }
        check(
            preferences.edit()
                .putString(CIPHERTEXT, Base64.encodeToString(encrypted.ciphertext, Base64.NO_WRAP))
                .putString(IV, Base64.encodeToString(encrypted.iv, Base64.NO_WRAP))
                .commit(),
        ) { "failed to persist encrypted device credential" }
        mutableConnected.value = true
    }

    @Synchronized
    override fun currentCredential(): String? {
        val ciphertext = preferences.getString(CIPHERTEXT, null) ?: run {
            mutableConnected.value = false
            return null
        }
        val iv = preferences.getString(IV, null) ?: return discardUnusableCredential()
        return runCatching {
            val plaintext = cipher.decrypt(
                Base64.decode(ciphertext, Base64.NO_WRAP),
                Base64.decode(iv, Base64.NO_WRAP),
            )
            try {
                plaintext.toString(Charsets.UTF_8)
                    .takeIf { it.isNotBlank() } ?: error("decrypted device credential is blank")
            } finally {
                plaintext.fill(0)
            }
        }.getOrElse {
            discardUnusableCredential()
        }.also { mutableConnected.value = it != null }
    }

    @Synchronized
    override fun clearIfCurrent(rejectedCredential: String): Boolean {
        if (currentCredential() != rejectedCredential) return false
        clear()
        return true
    }

    @Synchronized
    override fun clear() {
        clearCredentialPreferencesChecked()
        mutableConnected.value = false
    }

    private fun discardUnusableCredential(): String? {
        runCatching { preferences.edit().clear().commit() }
        runCatching { cipher.deleteKey() }
        mutableConnected.value = false
        return null
    }

    private fun clearCredentialPreferencesChecked() {
        check(preferences.edit().clear().commit()) { "failed to clear device credential" }
        mutableConnected.value = false
    }

    private class AndroidKeystoreCredentialCipher(private val keyAlias: String) : CredentialCipher {
        private val keyStore = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }

        override fun encrypt(plaintext: ByteArray): EncryptedPayload {
            val platformCipher = Cipher.getInstance(TRANSFORMATION)
            platformCipher.init(Cipher.ENCRYPT_MODE, getOrCreateKey())
            return EncryptedPayload(platformCipher.doFinal(plaintext), platformCipher.iv)
        }

        override fun decrypt(ciphertext: ByteArray, iv: ByteArray): ByteArray {
            val key = keyStore.getKey(keyAlias, null) as? SecretKey
                ?: error("device credential key is unavailable")
            val platformCipher = Cipher.getInstance(TRANSFORMATION)
            platformCipher.init(Cipher.DECRYPT_MODE, key, GCMParameterSpec(128, iv))
            return platformCipher.doFinal(ciphertext)
        }

        override fun deleteKey() {
            if (keyStore.containsAlias(keyAlias)) keyStore.deleteEntry(keyAlias)
        }

        private fun getOrCreateKey(): SecretKey {
            (keyStore.getKey(keyAlias, null) as? SecretKey)?.let { return it }
            return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").run {
                init(
                    KeyGenParameterSpec.Builder(
                        keyAlias,
                        KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
                    ).setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                        .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                        .setRandomizedEncryptionRequired(true)
                        .build(),
                )
                generateKey()
            }
        }
    }

    companion object {
        internal const val PREFERENCES = "secure_device_credential"
        private const val KEY_ALIAS = "training_sensors_device_credential_v1"
        private const val TRANSFORMATION = "AES/GCM/NoPadding"
        private const val CIPHERTEXT = "credential_ciphertext"
        private const val IV = "credential_iv"
    }
}
