package com.vineyardfinance.sensors.collector

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.time.OffsetDateTime

class H10ProtocolTest {
    @Test fun operationTimeoutRemainsARecoverableConnectionCycleFailure() = runTest {
        val timeout = runCatching { withTimeout(1) { delay(100) } }.exceptionOrNull()!!
        var recovered: Throwable? = null

        H10Collector.handleConnectionCycleFailure(timeout) { recovered = it }

        assertEquals(timeout, recovered)
    }

    @Test fun ordinaryGattFailureStillRecordsReconnectEventAndStatus() = runTest {
        val gattFailure = IOException("GATT connection lost")
        var eventDetail: String? = null
        var status: String? = null

        H10Collector.handleConnectionCycleFailure(gattFailure) { error ->
            eventDetail = error.message
            status = "Reconnecting"
        }

        assertEquals("GATT connection lost", eventDetail)
        assertEquals("Reconnecting", status)
    }

    @Test fun archiveCriticalLowStorageEscapesReconnectAndLatchesActionRequiredAfterCleanup() = runTest {
        var criticalStorageEventRecorded = false
        var recoverableEventOrStatusUpdate = false
        var connectionCleanupRan = false

        val propagated = runCatching {
            try {
                H10Collector.requireArchiveCapacity(128L * 1024L * 1024L) {
                    criticalStorageEventRecorded = true
                }
            } catch (error: Throwable) {
                H10Collector.handleConnectionCycleFailure(error) {
                    recoverableEventOrStatusUpdate = true
                }
            } finally {
                connectionCleanupRan = true
            }
        }.exceptionOrNull()

        assertTrue(propagated is H10Collector.ArchiveCriticalStorageFailure)
        val storageFailure = propagated as H10Collector.ArchiveCriticalStorageFailure
        assertTrue(criticalStorageEventRecorded)
        assertFalse(recoverableEventOrStatusUpdate)
        assertTrue(connectionCleanupRan)
        assertEquals(128L * 1024L * 1024L, storageFailure.freeBytes)
        assertEquals(H10Collector.MIN_FREE_BYTES, storageFailure.minimumFreeBytes)
        assertTrue(storageFailure.message.orEmpty().contains("256 MiB safety threshold"))
        assertTrue(storageFailure.message.orEmpty().contains("134217728 bytes free"))

        val decision = CollectorRuntimePolicy.afterFatalFailure(collectionDesired = true)
        val status = CollectorRuntimePolicy.failedStatus(CollectorSnapshot(), storageFailure.message!!)
        assertEquals(CollectorServiceDirective.HOLD_FAILED_FOREGROUND, decision.directive)
        assertTrue(decision.keepForeground)
        assertFalse(decision.scheduleResidualUpload)
        assertEquals("Collection failed", status.state)
        assertTrue(status.detail.startsWith("Action required:"))
        assertTrue(status.serviceRunning)
        assertFalse(status.connected)
        assertEquals(storageFailure.message, status.lastError)
    }

    @Test fun archiveCapacityAtThresholdDoesNotEmitCriticalEventOrStopCollection() = runTest {
        var criticalEventRecorded = false

        H10Collector.requireArchiveCapacity(H10Collector.MIN_FREE_BYTES) {
            criticalEventRecorded = true
        }

        assertFalse(criticalEventRecorded)
    }

    @Test fun criticalEventWriteFailureCannotDowngradeTheStorageSafetyStop() = runTest {
        val eventWriteFailure = IOException("queue insert failed while recording critical storage")

        val propagated = runCatching {
            H10Collector.requireArchiveCapacity(freeBytes = 0) {
                throw eventWriteFailure
            }
        }.exceptionOrNull()

        assertTrue(propagated is H10Collector.ArchiveCriticalStorageFailure)
        assertEquals(listOf(eventWriteFailure), propagated!!.suppressed.toList())
    }

    @Test fun streamCommandsMatchProvenH10Settings() {
        assertArrayEquals(byteArrayOf(0x02, 0x00, 0x00, 0x01, 0x82.toByte(), 0, 1, 1, 0x0e, 0), H10Protocol.ECG_START)
        assertEquals(14, H10Protocol.ACC_START.size)
        assertArrayEquals(byteArrayOf(0x03, 0x00), H10Protocol.ECG_STOP)
        assertArrayEquals(byteArrayOf(0x03, 0x02), H10Protocol.ACC_STOP)
    }

    @Test fun controlResponseMatchesOpcodeAndMeasurement() {
        val response = byteArrayOf(0xf0.toByte(), 0x02, 0x00, 0x00, 0x00)
        assertTrue(H10Protocol.isMatchingControlResponse(response, H10Protocol.ECG_START))
        assertTrue(H10Protocol.controlSucceeded(response))
        assertFalse(H10Protocol.isMatchingControlResponse(response, H10Protocol.ACC_START))
        assertFalse(H10Protocol.controlSucceeded(response.copyOf().apply { this[3] = 5 }))
    }

    @Test fun psftpEmptyQueryFixtureMatchesRustCollector() {
        val packets = H10Protocol.psftpPackets(3, byteArrayOf())
        assertEquals(1, packets.size)
        assertArrayEquals(byteArrayOf(0x02, 0x03, 0x80.toByte()), packets.single())
    }

    @Test fun psftpLocalTimePreservesOffsetAndMilliseconds() {
        val result = H10Protocol.psftpLocalTime(OffsetDateTime.parse("2026-05-24T12:13:14.123+01:00"))
        assertEquals(60, result.offsetMinutes)
        assertTrue(result.parameters.isNotEmpty())
        assertTrue(result.packets.isNotEmpty())
        assertEquals("2026-05-24T12:13:14.123+01:00", result.iso8601)
    }

    @Test fun psftpResponseAssemblerAcceptsMultiFrameAndSuccessCode() {
        val assembler = H10Protocol.PsftpResponseAssembler()
        assertNull(assembler.accept(byteArrayOf(0x06, 1, 2)).getOrThrow())
        assertArrayEquals(byteArrayOf(1, 2, 3), assembler.accept(byteArrayOf(0x13, 3)).getOrThrow())
        assertArrayEquals(byteArrayOf(), H10Protocol.PsftpResponseAssembler().accept(byteArrayOf(0, 0, 0)).getOrThrow())
    }

    @Test fun psftpResponseAssemblerRejectsSequenceAndDeviceErrors() {
        assertTrue(H10Protocol.PsftpResponseAssembler().accept(byteArrayOf(0x10)).isFailure)
        assertTrue(H10Protocol.PsftpResponseAssembler().accept(byteArrayOf(0, 6, 0)).isFailure)
        assertTrue(H10Protocol.PsftpResponseAssembler().accept(byteArrayOf()).isFailure)
    }
}
