package com.vineyardfinance.sensors.collector

import java.time.OffsetDateTime
import java.util.UUID

object H10Protocol {
    fun uuid(value: String): UUID = UUID.fromString(value)

    val HR_MEASUREMENT: UUID = uuid("00002a37-0000-1000-8000-00805f9b34fb")
    val BATTERY_LEVEL: UUID = uuid("00002a19-0000-1000-8000-00805f9b34fb")
    val MANUFACTURER_NAME: UUID = uuid("00002a29-0000-1000-8000-00805f9b34fb")
    val MODEL_NUMBER: UUID = uuid("00002a24-0000-1000-8000-00805f9b34fb")
    val SERIAL_NUMBER: UUID = uuid("00002a25-0000-1000-8000-00805f9b34fb")
    val HARDWARE_REVISION: UUID = uuid("00002a27-0000-1000-8000-00805f9b34fb")
    val FIRMWARE_REVISION: UUID = uuid("00002a26-0000-1000-8000-00805f9b34fb")
    val SOFTWARE_REVISION: UUID = uuid("00002a28-0000-1000-8000-00805f9b34fb")
    val PMD_CONTROL: UUID = uuid("fb005c81-02e7-f387-1cad-8acd2d8df0c8")
    val PMD_DATA: UUID = uuid("fb005c82-02e7-f387-1cad-8acd2d8df0c8")
    val PSFTP_MTU: UUID = uuid("fb005c51-02e7-f387-1cad-8acd2d8df0c8")
    val CCCD: UUID = uuid("00002902-0000-1000-8000-00805f9b34fb")

    val ECG_START = byteArrayOf(0x02, 0x00, 0x00, 0x01, 0x82.toByte(), 0x00, 0x01, 0x01, 0x0e, 0x00)
    val ACC_START = byteArrayOf(0x02, 0x02, 0x00, 0x01, 0xc8.toByte(), 0x00, 0x01, 0x01, 0x10, 0x00, 0x02, 0x01, 0x08, 0x00)
    val ECG_STOP = byteArrayOf(0x03, 0x00)
    val ACC_STOP = byteArrayOf(0x03, 0x02)

    fun isMatchingControlResponse(payload: ByteArray, command: ByteArray): Boolean =
        payload.size >= 4 && command.size >= 2 &&
            payload[0] == 0xf0.toByte() && payload[1] == command[0] && payload[2] == command[1]

    fun controlSucceeded(payload: ByteArray): Boolean = payload.size >= 4 && payload[3] == 0.toByte()

    fun psftpLocalTime(now: OffsetDateTime = OffsetDateTime.now()): PsftpLocalTime {
        val params = mutableListOf<Byte>()
        val date = mutableListOf<Byte>().apply {
            uint32(1, now.year)
            uint32(2, now.monthValue)
            uint32(3, now.dayOfMonth)
        }.toByteArray()
        val time = mutableListOf<Byte>().apply {
            uint32(1, now.hour)
            uint32(2, now.minute)
            uint32(3, now.second)
            uint32(4, now.nano / 1_000_000)
        }.toByteArray()
        params.message(1, date)
        params.message(2, time)
        val offsetMinutes = now.offset.totalSeconds / 60
        params.int32(3, offsetMinutes)
        val parameterBytes = params.toByteArray()
        return PsftpLocalTime(
            iso8601 = now.toString(),
            unixNs = Math.addExact(Math.multiplyExact(now.toEpochSecond(), 1_000_000_000L), now.nano.toLong()),
            offsetMinutes = offsetMinutes,
            parameters = parameterBytes,
            packets = psftpPackets(3, parameterBytes),
        )
    }

    fun psftpPackets(queryId: Int, parameters: ByteArray, mtuPayload: Int = 20): List<ByteArray> {
        require(mtuPayload >= 2)
        val stream = byteArrayOf((queryId and 0xff).toByte(), (((queryId ushr 8) and 0xff) or 0x80).toByte()) + parameters
        val packets = mutableListOf<ByteArray>()
        var offset = 0
        var next = 0
        var sequence = 0
        do {
            val take = minOf(stream.size - offset, mtuPayload - 1)
            val more = stream.size - offset > mtuPayload - 1
            val status = if (more) 0x06 else 0x02
            packets += byteArrayOf((status or next or (sequence shl 4)).toByte()) + stream.copyOfRange(offset, offset + take)
            offset += take
            next = 1
            sequence = (sequence + 1) and 0x0f
        } while (offset < stream.size)
        return packets
    }

    class PsftpResponseAssembler {
        private var expectedSequence = 0
        private var expectedNext = 0
        private val bytes = mutableListOf<Byte>()

        fun accept(frame: ByteArray): Result<ByteArray?> = runCatching {
            require(frame.isNotEmpty()) { "empty PS-FTP response frame" }
            val header = frame[0].toInt() and 0xff
            val next = header and 0x01
            val status = (header and 0x06) ushr 1
            val sequence = header ushr 4
            require(sequence == expectedSequence) { "PS-FTP sequence mismatch" }
            require(next == expectedNext) { "PS-FTP next-frame mismatch" }
            expectedSequence = (expectedSequence + 1) and 0x0f
            expectedNext = 1
            when (status) {
                3 -> { bytes += frame.drop(1); null }
                1 -> { bytes += frame.drop(1); bytes.toByteArray() }
                0 -> {
                    if (frame.size == 3) {
                        val error = (frame[1].toInt() and 0xff) or ((frame[2].toInt() and 0xff) shl 8)
                        require(error == 0) { "PS-FTP response error $error" }
                    }
                    bytes.toByteArray()
                }
                else -> error("unexpected PS-FTP status $status")
            }
        }
    }

    data class PsftpLocalTime(
        val iso8601: String,
        val unixNs: Long,
        val offsetMinutes: Int,
        val parameters: ByteArray,
        val packets: List<ByteArray>,
    )

    private fun MutableList<Byte>.message(field: Int, value: ByteArray) {
        add(((field shl 3) or 2).toByte()); varint(value.size.toLong()); addAll(value.toList())
    }
    private fun MutableList<Byte>.uint32(field: Int, value: Int) { add((field shl 3).toByte()); varint(value.toLong()) }
    private fun MutableList<Byte>.int32(field: Int, value: Int) { add((field shl 3).toByte()); varint(value.toLong()) }
    private fun MutableList<Byte>.varint(input: Long) {
        var value = input
        while (value and -128L != 0L) { add(((value and 0x7f) or 0x80).toByte()); value = value ushr 7 }
        add(value.toByte())
    }
}
