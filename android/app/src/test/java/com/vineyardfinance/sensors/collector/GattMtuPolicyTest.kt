package com.vineyardfinance.sensors.collector

import android.bluetooth.BluetoothGatt
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class GattMtuPolicyTest {
    @Test fun acceptsLargeSuccessfulMtu() {
        assertEquals(247, GattConnection.validateMtuResult(247, BluetoothGatt.GATT_SUCCESS))
    }

    @Test fun rejectsSmallOrFailedMtuLoudly() {
        val small = runCatching {
            GattConnection.validateMtuResult(23, BluetoothGatt.GATT_SUCCESS)
        }.exceptionOrNull()
        val failed = runCatching {
            GattConnection.validateMtuResult(247, BluetoothGatt.GATT_FAILURE)
        }.exceptionOrNull()
        assertTrue(small?.message.orEmpty().contains("below required"))
        assertTrue(failed?.message.orEmpty().contains("status"))
    }

    @Test fun negotiationTimeoutIsStrictlyBounded() {
        assertTrue(GattConnection.MTU_TIMEOUT_MS in 1..10_000)
        assertTrue(GattConnection.REQUESTED_MTU >= GattConnection.MIN_REQUIRED_MTU)
    }
}
