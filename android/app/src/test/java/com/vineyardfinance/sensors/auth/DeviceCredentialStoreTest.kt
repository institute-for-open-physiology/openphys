package com.vineyardfinance.sensors.auth

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.data.AppDatabase
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.data.QueueRepository
import com.vineyardfinance.sensors.data.RawRecord
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class DeviceCredentialStoreTest {
    private lateinit var context: Context
    private lateinit var database: AppDatabase

    @Before fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        context.getSharedPreferences(EncryptedDeviceCredentialStore.PREFERENCES, Context.MODE_PRIVATE)
            .edit().clear().commit()
        context.getSharedPreferences("installation_identity", Context.MODE_PRIVATE).edit().clear().commit()
        database = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
    }

    @After fun tearDown() {
        database.close()
    }

    @Test fun invalidAliasIsDeletedAndReEnrollmentKeepsInstallationAndQueue() = runTest {
        val identity = AppIdentity(context)
        val installationId = identity.installationId
        val queue = QueueRepository(database, identity)
        queue.append(
            CaptureContext(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
            record(),
        )
        val cipher = InvalidatableCipher()
        val store = EncryptedDeviceCredentialStore(context, cipher)
        store.storeCredential("old-device-credential")

        cipher.invalid = true
        assertNull(store.currentCredential())

        assertEquals(1, cipher.deletedKeys)
        assertFalse(store.connected.value)
        assertTrue(
            context.getSharedPreferences(EncryptedDeviceCredentialStore.PREFERENCES, Context.MODE_PRIVATE)
                .all.isEmpty(),
        )
        assertEquals(installationId, identity.installationId)
        assertEquals(1, queue.backlog().records)

        store.storeCredential("replacement-device-credential")
        assertEquals("replacement-device-credential", store.currentCredential())
        assertTrue(store.connected.value)
        assertEquals(installationId, identity.installationId)
        assertEquals(1, queue.backlog().records)
    }

    @Test fun encryptionKeyFailureResetsAliasAndRetriesOnce() {
        val cipher = InvalidatableCipher().apply { invalid = true }
        val store = EncryptedDeviceCredentialStore(context, cipher)

        store.storeCredential("fresh-device-credential")

        assertEquals(1, cipher.deletedKeys)
        assertEquals("fresh-device-credential", store.currentCredential())
    }

    @Test fun encryptedRoundTripCompareAndClearNeverPersistsPlaintext() {
        val cipher = InvalidatableCipher()
        val store = EncryptedDeviceCredentialStore(context, cipher)

        store.storeCredential("secret-device-credential")

        assertEquals("secret-device-credential", store.currentCredential())
        assertTrue(store.connected.value)
        val preferences = context.getSharedPreferences(
            EncryptedDeviceCredentialStore.PREFERENCES,
            Context.MODE_PRIVATE,
        )
        assertFalse(preferences.all.values.any { it.toString().contains("secret-device-credential") })
        assertFalse(store.clearIfCurrent("stale-device-credential"))
        assertEquals("secret-device-credential", store.currentCredential())
        assertTrue(store.clearIfCurrent("secret-device-credential"))
        assertNull(store.currentCredential())
        assertFalse(store.connected.value)
    }

    @Test fun missingIvIsTreatedAsInvalidKeyStateAndCanBeReplaced() {
        val cipher = InvalidatableCipher()
        val store = EncryptedDeviceCredentialStore(context, cipher)
        store.storeCredential("credential-before-corruption")
        val preferences = context.getSharedPreferences(
            EncryptedDeviceCredentialStore.PREFERENCES,
            Context.MODE_PRIVATE,
        )
        assertTrue(preferences.edit().remove("credential_iv").commit())

        assertNull(store.currentCredential())

        assertEquals(1, cipher.deletedKeys)
        assertTrue(preferences.all.isEmpty())
        store.storeCredential("credential-after-corruption")
        assertEquals("credential-after-corruption", store.currentCredential())
    }

    @Test fun blankCredentialIsRejectedWithoutPersistingState() {
        val store = EncryptedDeviceCredentialStore(context, InvalidatableCipher())

        assertTrue(runCatching { store.storeCredential("   ") }.isFailure)

        assertNull(store.currentCredential())
        assertFalse(store.connected.value)
    }

    private fun record() = RawRecord(
        connectionId = null,
        collectorUnixNs = 1,
        collectorMonotonicNs = 2,
        physicalDeviceId = "h10_test",
        blePeripheralId = null,
        kind = "collector_event",
        body = JsonObject().apply { addProperty("event", "queued-before-key-invalidation") },
    )

    private class InvalidatableCipher : EncryptedDeviceCredentialStore.CredentialCipher {
        var invalid = false
        var deletedKeys = 0
        private var mask = 0x5a

        override fun encrypt(plaintext: ByteArray): EncryptedDeviceCredentialStore.EncryptedPayload {
            check(!invalid) { "simulated invalid key alias" }
            return EncryptedDeviceCredentialStore.EncryptedPayload(
                plaintext.map { (it.toInt() xor mask).toByte() }.toByteArray(),
                byteArrayOf(mask.toByte()),
            )
        }

        override fun decrypt(ciphertext: ByteArray, iv: ByteArray): ByteArray {
            check(!invalid) { "simulated permanently invalidated key" }
            check(iv.contentEquals(byteArrayOf(mask.toByte()))) { "wrong simulated IV" }
            return ciphertext.map { (it.toInt() xor mask).toByte() }.toByteArray()
        }

        override fun deleteKey() {
            deletedKeys += 1
            invalid = false
            mask = (mask + 1) and 0xff
        }
    }
}
