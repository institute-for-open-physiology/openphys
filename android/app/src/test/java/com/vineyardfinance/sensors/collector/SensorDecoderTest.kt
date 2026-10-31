package com.vineyardfinance.sensors.collector

import com.google.gson.JsonParser
import org.junit.Assert.*
import org.junit.Test

class SensorDecoderTest {
    @Test
    fun sharedBeatTimings() {
        val row = fixtures.getAsJsonObject("beats")
        val detector = BeatDetector(row["rate"].asDouble)
        val found =
            row["samples"]
                .asJsonArray
                .mapIndexedNotNull { index, value -> detector.push(value.asFloat, index) }
                .filter { it > 320 }
        val expected = row["expected"].asJsonArray.map { it.asInt }
        assertEquals(expected.size, found.size)
        found.zip(expected).forEach { (a, b) ->
            assertTrue("$a vs $b", kotlin.math.abs(a - b) <= 1)
        }
    }

    private val fixtures =
        JsonParser.parseReader(javaClass.getResourceAsStream("/sensor.json")!!.reader())
            .asJsonObject

    @Test
    fun sharedHeartRatePackets() {
        fixtures.getAsJsonArray("heartRate").forEach { element ->
            val row = element.asJsonObject
            val name = row["name"].asString
            val sample =
                SensorDecoder.parseHeartRate(
                    row["bytes"].asJsonArray.map { it.asInt.toByte() }.toByteArray()
                )
            if (row["bpm"].isJsonNull) assertNull(name, sample)
            else {
                requireNotNull(sample)
                assertEquals(name, row["bpm"].asInt, sample.bpm)
                assertEquals(name, row["supported"].asBoolean, sample.contactSupported)
                assertEquals(name, row["detected"].asBoolean, sample.contactDetected)
                assertEquals(name, row["rr"].asJsonArray.map { it.asDouble }, sample.rrMs)
                assertEquals(
                    name,
                    if (row["contact"].isJsonNull) null else row["contact"].asBoolean,
                    sample.contact(row["age"].asDouble),
                )
            }
        }
    }

    @Test
    fun sharedMeasurementPackets() {
        fixtures.getAsJsonArray("pmd").forEach { element ->
            val row = element.asJsonObject
            val name = row["name"].asString
            val frame =
                SensorDecoder.parseFrame(
                    row["bytes"].asJsonArray.map { it.asInt.toByte() }.toByteArray()
                )
            assertEquals(name, row["valid"].asBoolean, frame != null)
            if (frame != null) {
                assertEquals(
                    name,
                    if (row["ecg"].isJsonNull) null else row["ecg"].asJsonArray.map { it.asInt },
                    SensorDecoder.decodeECG(frame),
                )
                assertEquals(
                    name,
                    if (row["acc"].isJsonNull) null
                    else row["acc"].asJsonArray.map { it.asJsonArray.map { n -> n.asInt } },
                    SensorDecoder.decodeACC(frame),
                )
            }
        }
    }
}
