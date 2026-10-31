package com.vineyardfinance.sensors

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.room.Room
import com.google.gson.JsonObject
import com.vineyardfinance.sensors.auth.EncryptedDeviceCredentialStore
import com.vineyardfinance.sensors.data.AppDatabase
import com.vineyardfinance.sensors.data.AppIdentity
import com.vineyardfinance.sensors.data.CaptureContext
import com.vineyardfinance.sensors.data.QueueRepository
import com.vineyardfinance.sensors.data.RawRecord
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class DeviceStorageInstrumentedTest {
    @Test fun databaseUsesWalAndFullSynchronousDurability() {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        val name = "sensor-spool-instrumentation-${UUID.randomUUID()}.sqlite3"
        val database = AppDatabase.build(context, name)
        try {
            val sqlite = database.openHelper.writableDatabase
            sqlite.query("PRAGMA journal_mode").use { cursor -> assertTrue(cursor.moveToFirst()); assertEquals("wal", cursor.getString(0).lowercase()) }
            sqlite.query("PRAGMA synchronous").use { cursor -> assertTrue(cursor.moveToFirst()); assertEquals(2, cursor.getInt(0)) }
        } finally {
            database.close()
            assertTrue(context.deleteDatabase(name))
        }
    }

    @Test fun deviceCredentialRoundTripsOnlyThroughAndroidKeystoreAndClearsWithCompareAndSet() {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        val preferencesName = "secure-device-credential-instrumentation"
        assertTrue(context.getSharedPreferences(preferencesName, android.content.Context.MODE_PRIVATE).edit().clear().commit())
        val store = EncryptedDeviceCredentialStore(context, preferencesName)
        store.clear()
        store.storeCredential("secret-test-token")
        assertTrue(store.connected.value)
        assertEquals("secret-test-token", store.currentCredential())

        val preferences = context.getSharedPreferences(
            preferencesName,
            android.content.Context.MODE_PRIVATE,
        )
        assertFalse(preferences.all.keys.any { it.contains("secret-test-token") })
        assertFalse(preferences.all.values.any { it.toString().contains("secret-test-token") })

        assertFalse(store.clearIfCurrent("a-stale-credential"))
        assertEquals("secret-test-token", store.currentCredential())
        assertTrue(store.clearIfCurrent("secret-test-token"))
        assertFalse(store.connected.value)
        assertNull(store.currentCredential())
        assertTrue(preferences.edit().clear().commit())
    }

    @Test fun invalidAliasRecoveryRetainsInstallationAndQueuedRecords() = runBlocking {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        val credentialPreferences = "secure-device-credential-invalidation-instrumentation"
        val identityPreferences = "installation-identity-invalidation-instrumentation"
        assertTrue(context.getSharedPreferences(credentialPreferences, android.content.Context.MODE_PRIVATE).edit().clear().commit())
        assertTrue(context.getSharedPreferences(identityPreferences, android.content.Context.MODE_PRIVATE).edit().clear().commit())
        val identity = AppIdentity(context, identityPreferences)
        val originalInstallationId = identity.installationId
        val database = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        try {
            val queue = QueueRepository(database, identity)
            queue.append(
                CaptureContext(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
                RawRecord(
                    connectionId = null,
                    collectorUnixNs = 1,
                    collectorMonotonicNs = 2,
                    physicalDeviceId = "h10_test",
                    blePeripheralId = null,
                    kind = "collector_event",
                    body = JsonObject().apply { addProperty("event", "queued-before-key-invalidation") },
                ),
            )
            val cipher = InvalidatableCipher()
            val store = EncryptedDeviceCredentialStore(context, cipher, credentialPreferences)
            store.storeCredential("credential-before-invalidation")

            cipher.invalid = true
            assertNull(store.currentCredential())
            assertEquals(1, cipher.deletedKeys)
            assertEquals(originalInstallationId, identity.installationId)
            assertEquals(1, queue.backlog().records)

            store.storeCredential("replacement-credential")
            assertEquals("replacement-credential", store.currentCredential())
            assertEquals(originalInstallationId, identity.installationId)
            assertEquals(1, queue.backlog().records)
        } finally {
            database.close()
            assertTrue(context.getSharedPreferences(credentialPreferences, android.content.Context.MODE_PRIVATE).edit().clear().commit())
            assertTrue(context.getSharedPreferences(identityPreferences, android.content.Context.MODE_PRIVATE).edit().clear().commit())
        }
    }

    private class InvalidatableCipher : EncryptedDeviceCredentialStore.CredentialCipher {
        var invalid = false
        var deletedKeys = 0
        private var mask = 0x41

        override fun encrypt(plaintext: ByteArray): EncryptedDeviceCredentialStore.EncryptedPayload {
            check(!invalid) { "simulated invalid key alias" }
            return EncryptedDeviceCredentialStore.EncryptedPayload(
                plaintext.map { (it.toInt() xor mask).toByte() }.toByteArray(),
                byteArrayOf(mask.toByte()),
            )
        }

        override fun decrypt(ciphertext: ByteArray, iv: ByteArray): ByteArray {
            check(!invalid) { "simulated permanently invalidated key" }
            check(iv.contentEquals(byteArrayOf(mask.toByte())))
            return ciphertext.map { (it.toInt() xor mask).toByte() }.toByteArray()
        }

        override fun deleteKey() {
            deletedKeys += 1
            invalid = false
            mask += 1
        }
    }
}
