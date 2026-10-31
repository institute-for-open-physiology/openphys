package com.vineyardfinance.sensors.collector

import android.app.Application
import androidx.test.core.app.ApplicationProvider
import com.vineyardfinance.sensors.data.*
import org.junit.*
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(application = Application::class)
class LiveSignalsTest {
    @Test
    fun measurementBuffersStayBoundedAndClockUsesAReceipt() {
        val fixtures =
            com.google.gson.JsonParser.parseReader(
                    javaClass.getResourceAsStream("/sensor.json")!!.reader()
                )
                .asJsonObject
        val samples =
            fixtures["beats"].asJsonObject["samples"].asJsonArray.map { it.asDouble.toInt() }
        val time = 1791543600.0
        assertFalse(signals.snapshot(time).clockSynced)
        signals.setClock(true, 0)
        repeat(2) { loop ->
            samples.chunked(130).forEachIndexed { index, chunk ->
                val packet =
                    java.nio.ByteBuffer.allocate(10 + chunk.size * 3)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                packet.put(0)
                packet.putLong(((time + loop * 21 + index - 946684800) * 1e9).toLong())
                packet.put(0)
                chunk.forEach { value ->
                    repeat(3) { byte -> packet.put((value shr (byte * 8)).toByte()) }
                }
                signals.receive(H10Protocol.PMD_DATA, packet.array(), time + loop * 21 + index)
            }
        }
        val acc =
            fixtures["pmd"]
                .asJsonArray
                .first { it.asJsonObject["name"].asString == "acc-16bit" }
                .asJsonObject["bytes"]
                .asJsonArray
                .map { it.asInt.toByte() }
                .toByteArray()
        repeat(10) { signals.receive(H10Protocol.PMD_DATA, acc, time + 42 + it / 10.0) }
        signals.receiveRSSI(-58, time + 42.9)
        signals.receive(H10Protocol.PMD_DATA, byteArrayOf(), time + 42.9)
        val snapshot = signals.snapshot(time + 43)
        assertEquals(3900, snapshot.ecg.size)
        assertTrue(snapshot.beats.isNotEmpty())
        assertNotNull(snapshot.acceleration)
        assertNotNull(snapshot.current[2])
        assertEquals(-58.0, snapshot.current[3])
        assertTrue(snapshot.clockSynced)
        signals.disconnect()
        assertFalse(signals.snapshot(time + 44).clockSynced)
        assertNull(signals.snapshot(time + 44).acceleration)
    }

    private lateinit var store: SignalStore
    private lateinit var signals: LiveSignals

    @Before
    fun open() {
        val app = ApplicationProvider.getApplicationContext<Application>()
        app.deleteDatabase("signal-history.sqlite3")
        store = SignalStore(app)
        signals = LiveSignals(store)
    }

    @After
    fun close() {
        store.close()
    }

    @Test
    fun contactAndHeartRateExpireAndRemovalClearsBoth() {
        signals.receive(H10Protocol.HR_MEASUREMENT, byteArrayOf(6, 72), 100.0)
        assertEquals(72, signals.snapshot(102.0).bpm)
        assertEquals(true, signals.snapshot(102.0).contact)
        assertNull(signals.snapshot(102.5).bpm)
        assertNull(signals.snapshot(102.5).contact)
        signals.receive(H10Protocol.HR_MEASUREMENT, byteArrayOf(4, 72), 103.0)
        assertNull(signals.snapshot(103.0).bpm)
        assertEquals(false, signals.snapshot(103.0).contact)
    }

    @Test
    fun disconnectSavesLastBinAndKeepsMissingIntervalsEmpty() {
        signals.receive(H10Protocol.HR_MEASUREMENT, byteArrayOf(6, 72), 100.0)
        signals.disconnect()
        store.close()
        store = SignalStore(ApplicationProvider.getApplicationContext())
        val bins = store.readBins(100.0, 115.0, 3)
        assertEquals(3, bins.size)
        assertEquals(72.0, bins[0].values[0])
        assertNull(bins[1].values[0])
        assertNull(bins[2].values[0])
        assertEquals(bins, mergeCurrentBin(bins, HistoryBin(20.0, List(5) { 1.0 }), 100.0, 115.0))
    }
}
