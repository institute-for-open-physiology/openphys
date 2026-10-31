import Foundation
import XCTest
@testable import H10Kit

final class ProtocolTests: XCTestCase {
    func testStreamCommandsMatchAndroidCollector() {
        XCTAssertEqual(H10Protocol.ecgStart, [0x02, 0x00, 0x00, 0x01, 0x82, 0, 1, 1, 0x0e, 0])
        XCTAssertEqual(H10Protocol.accStart.count, 14)
        XCTAssertEqual(H10Protocol.ecgStop, [0x03, 0x00])
        XCTAssertEqual(H10Protocol.accStop, [0x03, 0x02])
    }

    func testControlResponseMatchesOpcodeAndMeasurement() {
        let response: [UInt8] = [0xf0, 0x02, 0x00, 0x00, 0x00]
        XCTAssertTrue(H10Protocol.isMatchingControlResponse(response, command: H10Protocol.ecgStart))
        XCTAssertTrue(H10Protocol.controlSucceeded(response))
        XCTAssertFalse(H10Protocol.isMatchingControlResponse(response, command: H10Protocol.accStart))
        XCTAssertFalse(H10Protocol.controlSucceeded([0xf0, 0x02, 0x00, 0x05, 0x00]))
    }

    func testPsftpEmptyQueryMatchesRustCollector() {
        XCTAssertEqual(H10Protocol.psftpPackets(queryId: 3, parameters: []), [[0x02, 0x03, 0x80]])
    }

    func testPsftpLocalTimeEncoding() {
        var utc = Calendar(identifier: .gregorian)
        utc.timeZone = TimeZone(secondsFromGMT: 0)!
        let base = utc.date(from: DateComponents(year: 2026, month: 5, day: 24, hour: 11, minute: 13, second: 14))!
        let time = H10Protocol.psftpLocalTime(date: base.addingTimeInterval(0.123), timeZone: TimeZone(secondsFromGMT: 3600)!)
        XCTAssertEqual(time.iso8601, "2026-05-24T12:13:14.123+01:00")
        XCTAssertEqual(time.offsetMinutes, 60)
        XCTAssertEqual((time.unixNs / 1_000_000) % 1000, 123)
        XCTAssertEqual(time.parameters, [0x0a, 0x07, 0x08, 0xea, 0x0f, 0x10, 0x05, 0x18, 0x18,
                                         0x12, 0x08, 0x08, 0x0c, 0x10, 0x0d, 0x18, 0x0e, 0x20, 0x7b,
                                         0x18, 0x3c])
        XCTAssertEqual(time.packets.count, 2)
        XCTAssertEqual(time.packets[0].count, 20)
        XCTAssertEqual(Array(time.packets[0].prefix(3)), [0x06, 0x03, 0x80])
        XCTAssertEqual(time.packets[1], [0x13, 0x20, 0x7b, 0x18, 0x3c])
    }

    func testPsftpResponseAssembler() throws {
        var assembler = H10Protocol.PsftpResponseAssembler()
        XCTAssertNil(try assembler.accept([0x06, 1, 2]))
        XCTAssertEqual(try assembler.accept([0x13, 3]), [1, 2, 3])
        var empty = H10Protocol.PsftpResponseAssembler()
        XCTAssertEqual(try empty.accept([0, 0, 0]), [])
        var badSequence = H10Protocol.PsftpResponseAssembler()
        XCTAssertThrowsError(try badSequence.accept([0x10]))
        var deviceError = H10Protocol.PsftpResponseAssembler()
        XCTAssertThrowsError(try deviceError.accept([0, 6, 0]))
        var emptyFrame = H10Protocol.PsftpResponseAssembler()
        XCTAssertThrowsError(try emptyFrame.accept([]))
    }

    func testHeartRateParsing() {
        let sample = H10Protocol.parseHeartRate([0x16, 60, 0x00, 0x04])
        XCTAssertEqual(sample, H10Protocol.HeartRateSample(bpm: 60, contactSupported: true, contactDetected: true, rrMs: [1000]))
        XCTAssertNil(H10Protocol.parseHeartRate([0x00]))
    }

    func testContactRequiresCurrentUsableReading() throws {
        let worn = try XCTUnwrap(H10Protocol.parseHeartRate([0x16, 72, 0x55, 0x03]))
        XCTAssertEqual(worn.contact(after: 0), true)
        XCTAssertEqual(worn.contact(after: 2.49), true)
        XCTAssertNil(worn.contact(after: 2.5))
        XCTAssertNil(worn.contact(after: 60))

        let removed = try XCTUnwrap(H10Protocol.parseHeartRate([0x04, 72]))
        XCTAssertEqual(removed.contact(after: 0), false)
        XCTAssertFalse(removed.hasHeartRate)

        let noBeat = try XCTUnwrap(H10Protocol.parseHeartRate([0x06, 0]))
        XCTAssertNil(noBeat.contact(after: 0))
        XCTAssertFalse(noBeat.hasHeartRate)

        let unsupported = try XCTUnwrap(H10Protocol.parseHeartRate([0x00, 72]))
        XCTAssertNil(unsupported.contact(after: 0))
        XCTAssertTrue(unsupported.hasHeartRate)
    }

    func testIdentityAndUUIDs() {
        XCTAssertEqual(H10Identity.serial(fromAdvertisedName: "Polar H10 A1B2C3D4"), "A1B2C3D4")
        XCTAssertNil(H10Identity.serial(fromAdvertisedName: "Polar OH1 A1B2C3D4"))
        XCTAssertEqual(H10Identity.normalizedSerial("a1b2c3d4\u{0}"), "A1B2C3D4")
        XCTAssertEqual(H10Identity.physicalDeviceId(serial: "A1B2C3D4"), "h10_a1b2c3d4")
        XCTAssertEqual(H10Protocol.fullUUID("2A37"), H10Protocol.heartRateMeasurement)
        XCTAssertEqual(H10Protocol.fullUUID("FB005C82-02E7-F387-1CAD-8ACD2D8DF0C8"), H10Protocol.pmdData)
    }

    func testRestoredSensorWithoutNameRequiresPreviouslyVerifiedIdentity() {
        XCTAssertTrue(H10Identity.matches(name: nil, configuredSerial: "A1B2C3D4", peripheralId: "ABC", savedPeripheralId: "abc"))
        XCTAssertFalse(H10Identity.matches(name: nil, configuredSerial: "A1B2C3D4", peripheralId: "DEF", savedPeripheralId: "ABC"))
        XCTAssertFalse(H10Identity.matches(name: nil, configuredSerial: "A1B2C3D4", peripheralId: "ABC", savedPeripheralId: nil))
        XCTAssertFalse(H10Identity.matches(name: nil, configuredSerial: nil, peripheralId: "ABC", savedPeripheralId: "ABC"))
    }

    func testAdvertisedIdentityOverridesSavedPeripheral() {
        XCTAssertFalse(H10Identity.matches(name: "Polar H10 AABBCCDD", configuredSerial: "A1B2C3D4", peripheralId: "ABC", savedPeripheralId: "ABC"))
        XCTAssertFalse(H10Identity.matches(name: "Polar OH1 A1B2C3D4", configuredSerial: "A1B2C3D4", peripheralId: "ABC", savedPeripheralId: "ABC"))
        XCTAssertTrue(H10Identity.matches(name: "Polar H10 A1B2C3D4", configuredSerial: "A1B2C3D4", peripheralId: "DEF", savedPeripheralId: "ABC"))
        XCTAssertTrue(H10Identity.matches(name: "Polar H10 A1B2C3D4", configuredSerial: nil, peripheralId: "DEF", savedPeripheralId: nil))
    }

    func testEnrollmentLinks() {
        let token = String(repeating: "aB3_-", count: 8) + "xyz"
        XCTAssertEqual(token.count, 43)
        XCTAssertEqual(EnrollmentLink.token(from: "https://sensors-poc.vineyard-finance.com/enroll#" + token), token)
        XCTAssertEqual(EnrollmentLink.token(from: "  " + token + "\n"), token)
        XCTAssertNil(EnrollmentLink.token(from: "http://sensors-poc.vineyard-finance.com/enroll#" + token))
        XCTAssertNil(EnrollmentLink.token(from: "https://sensors-poc.vineyard-finance.com/enroll?x=1#" + token))
        XCTAssertNil(EnrollmentLink.token(from: "https://evil.example/enroll#" + token))
    }
}

final class EncodingTests: XCTestCase {
    func testJSONEncodingEscapesAndKeepsOrder() {
        let value: JSONValue = ["a": "x\"\n\t\u{1}\\", "b": nil, "c": [1, true, 1.5], "d": [:]]
        XCTAssertEqual(value.encoded, "{\"a\":\"x\\\"\\n\\t\\u0001\\\\\",\"b\":null,\"c\":[1,true,1.5],\"d\":{}}")
    }

    func testSHA256Vectors() {
        XCTAssertEqual(Hashing.hex(PortableSHA256.digest([])), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        XCTAssertEqual(Hashing.hex(PortableSHA256.digest(Array("abc".utf8))), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        XCTAssertEqual(Hashing.hex(PortableSHA256.digest(Array("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".utf8))),
                       "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
    }
}

final class RecordStoreTests: XCTestCase {
    private let installation = "8b6c2a8e-6a1f-4a6e-9c55-0d6b9f0f1a10"
    private var directory: URL!

    override func setUp() {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    }

    override func tearDown() {
        try? FileManager.default.removeItem(at: directory)
    }

    private func record(_ index: Int) -> RawRecord {
        RawRecord(connectionId: nil, collectorUnixNs: 1_790_000_000_000_000_000 + Int64(index), collectorMonotonicNs: UInt64(index),
                  physicalDeviceId: "h10_a1b2c3d4", blePeripheralId: nil, kind: "collector_event",
                  body: ["event": "test", "severity": "info", "detail": "\(index)", "fields": [:]])
    }

    func testBatchesAckAndReloadAcrossSessions() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        let first = CaptureContext(physicalDeviceId: "h10_a1b2c3d4")
        for index in 0..<300 { store.append(context: first, record: record(index)) }

        let batch = try XCTUnwrap(store.nextBatch())
        XCTAssertEqual(batch.firstSourceSeq, 0)
        XCTAssertEqual(batch.lastSourceSeq, 255)
        XCTAssertEqual(batch.recordCount, 256)
        XCTAssertEqual(try store.nextBatch(), batch, "retries must resend identical bytes")
        XCTAssertEqual(batch.bodySha256, Hashing.hex(PortableSHA256.digest(Array(batch.body.utf8))))
        let parsed = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(batch.body.utf8)) as? [String: Any])
        XCTAssertEqual((parsed["records"] as? [Any])?.count, 256)
        XCTAssertEqual(parsed["source"] as? String, "chest_h10")

        XCTAssertThrowsError(try store.acknowledge(batch, committedThrough: 100))
        try store.acknowledge(batch, committedThrough: 255)

        let second = CaptureContext(physicalDeviceId: "h10_a1b2c3d4")
        store.append(context: second, record: record(300))
        XCTAssertEqual(store.backlog().records, 45)

        let reopened = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(reopened.backlog().records, 45)
        let tail = try XCTUnwrap(reopened.nextBatch())
        XCTAssertEqual(tail.firstSourceSeq, 256)
        XCTAssertEqual(tail.lastSourceSeq, 299)
        XCTAssertEqual(tail.captureSessionId, first.sessionId)
        try reopened.acknowledge(tail, committedThrough: 299)
        let next = try XCTUnwrap(reopened.nextBatch())
        XCTAssertEqual(next.firstSourceSeq, 300)
        XCTAssertEqual(next.captureSessionId, second.sessionId)
    }

    func testTornTailLineIsDropped() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        let context = CaptureContext(physicalDeviceId: nil)
        store.append(context: context, record: record(0))
        XCTAssertEqual(store.backlog().records, 1)
        let handle = try FileHandle(forWritingTo: directory.appendingPathComponent("pending.tsv"))
        try handle.seekToEnd()
        try handle.write(contentsOf: Data("abc\tdef\t1\t{\"record".utf8))
        try handle.close()

        let reopened = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(reopened.backlog().records, 1)
        reopened.append(context: context, record: record(1))
        XCTAssertEqual(try reopened.nextBatch()?.lastSourceSeq, 1)
    }

    func testAckValidation() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        store.append(context: CaptureContext(physicalDeviceId: nil), record: record(0))
        let batch = try XCTUnwrap(store.nextBatch())
        func ack(checksum: String) -> Data {
            Data(JSONValue.object([
                ("schema_version", 1), ("installation_id", .string(installation)), ("batch_id", .string(batch.batchId)),
                ("accepted_records", 1), ("durably_committed_through_seq", 0), ("missing_ranges", []),
                ("server_receive_unix_ms", 1), ("batch_checksum_sha256", .string(checksum)),
                ("encoded_bytes", .num(batch.encodedBytes)), ("record_count", 1),
            ]).encoded.utf8)
        }
        XCTAssertEqual(try IngestAck.committedThrough(responseBody: ack(checksum: batch.bodySha256), batch: batch), 0)
        XCTAssertThrowsError(try IngestAck.committedThrough(responseBody: ack(checksum: "00"), batch: batch))
        XCTAssertEqual(IngestAck.machineCode(Data("{\"code\":\"device_reconnect_required\"}".utf8)), "device_reconnect_required")
    }
}
