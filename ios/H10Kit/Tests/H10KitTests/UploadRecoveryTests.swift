import Foundation
import XCTest
@testable import H10Kit

final class UploadRecoveryTests: XCTestCase {
    private let installation = "8b6c2a8e-6a1f-4a6e-9c55-0d6b9f0f1a10"
    private var directory: URL!

    override func setUp() {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    }

    override func tearDown() { try? FileManager.default.removeItem(at: directory) }

    private func packet(_ timestamp: UInt64) -> [UInt8] {
        [0] + (0..<8).map { UInt8(truncatingIfNeeded: timestamp >> ($0 * 8)) } + [0, 1, 2, 3]
    }

    private func record(_ bytes: [UInt8], characteristic: String = H10Protocol.pmdData) -> RawRecord {
        RawRecord(connectionId: Ids.new(), collectorUnixNs: 1_791_407_988_646_783_000,
                  collectorMonotonicNs: 123_456_789, physicalDeviceId: "h10_test", blePeripheralId: Ids.new(),
                  kind: "gatt_notification", body: ["characteristic_uuid": .string(characteristic),
                    "description": "test", "payload_b64": .string(Data(bytes).base64EncodedString()), "payload_len": .num(bytes.count)])
    }

    private func records(_ batch: UploadBatch) throws -> [[String: Any]] {
        let body = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(batch.body.utf8)) as? [String: Any])
        return try XCTUnwrap(body["records"] as? [[String: Any]])
    }

    /// Reproduces an in-flight file written before invalid-packet handling existed.
    private func legacyBatch(_ records: [RawRecord]) throws -> (RecordStore, UploadBatch) {
        let context = CaptureContext(physicalDeviceId: "h10_test")
        let store = try RecordStore(directory: directory, installationId: installation)
        for record in records { store.append(context: context, record: record) }
        try store.flush()
        let id = Ids.new()
        let body = JSONValue.object([
            ("schema_version", 1), ("installation_id", .string(installation)), ("batch_id", .string(id)),
            ("source", "chest_h10"), ("capture_session_id", .string(context.sessionId)), ("writer_epoch_id", .string(context.epochId)),
            ("first_source_seq", 0), ("last_source_seq", .num(records.count - 1)),
            ("records", .array(records.enumerated().map { .raw(RecordJSON.encode(sourceSeq: Int64($0.offset), record: $0.element)) }))
        ]).encoded
        let batch = UploadBatch(batchId: id, installationId: installation, captureSessionId: context.sessionId,
                                writerEpochId: context.epochId, firstSourceSeq: 0, lastSourceSeq: Int64(records.count - 1),
                                recordCount: records.count, body: body, bodySha256: Hashing.sha256Hex(Array(body.utf8)))
        try JSONEncoder().encode(batch).write(to: directory.appendingPathComponent("batch.json"), options: .atomic)
        return (store, batch)
    }

    func testTimestampBoundaryAndTruncatedHeader() {
        XCTAssertNotNil(PMD.frame(packet(PMD.maximumTimestampNs)))
        XCTAssertNil(PMD.frame(packet(PMD.maximumTimestampNs + 1)))
        XCTAssertNil(PMD.frame([0, 1, 2]))
    }

    func testInvalidPacketKeepsExactOriginalAndIdentity() throws {
        for bytes in [packet(10_068_106_024_874_117_048), [0, 1, 2]] {
            let raw = record(bytes)
            let original = RecordJSON.encode(sourceSeq: 42, record: raw)
            let wrapped = try XCTUnwrap(RecordJSON.archiveInvalidPMD(original))
            let json = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(wrapped.utf8)) as? [String: Any])
            XCTAssertEqual(json["kind"] as? String, "collector_event")
            XCTAssertEqual(json["record_id"] as? String, raw.recordId)
            XCTAssertEqual(json["source_seq"] as? Int, 42)
            XCTAssertEqual((json["collector_unix_ns"] as? NSNumber)?.int64Value, raw.collectorUnixNs)
            let body = try XCTUnwrap(json["body"] as? [String: Any])
            let fields = try XCTUnwrap(body["fields"] as? [String: Any])
            XCTAssertEqual(fields["original_record_json"] as? String, original)
        }
    }

    func testValidPMDAndOtherCharacteristicsAreUnchanged() {
        for raw in [record(packet(PMD.maximumTimestampNs)),
                    record([0x16, 72], characteristic: H10Protocol.heartRateMeasurement),
                    record([0xf0, 2, 0, 0, 0], characteristic: H10Protocol.pmdControl)] {
            XCTAssertNil(RecordJSON.archiveInvalidPMD(RecordJSON.encode(sourceSeq: 0, record: raw)))
        }
    }

    func testRejectedBatchRecoverySurvivesRestartAndAck() throws {
        let good = record(packet(844_733_987_989_579_004))
        let bad = record(packet(10_068_106_024_874_117_048))
        let last = record(packet(844_733_987_989_579_005))
        let (store, rejected) = try legacyBatch([good, bad, last])
        XCTAssertEqual(try store.nextBatch(), rejected, "An ambiguous retry must retain its exact bytes.")
        let pending = try Data(contentsOf: directory.appendingPathComponent("pending.tsv"))
        XCTAssertTrue(try store.recoverRejectedBatch(rejected))
        let repaired = try XCTUnwrap(store.nextBatch())
        XCTAssertNotEqual(repaired.batchId, rejected.batchId)
        XCTAssertEqual(repaired.firstSourceSeq, rejected.firstSourceSeq)
        XCTAssertEqual(repaired.lastSourceSeq, rejected.lastSourceSeq)
        XCTAssertTrue(repaired.body.contains(RecordJSON.encode(sourceSeq: 0, record: good)))
        XCTAssertTrue(repaired.body.contains(RecordJSON.encode(sourceSeq: 2, record: last)))
        XCTAssertEqual(try records(repaired).map { $0["kind"] as? String }, ["gatt_notification", "collector_event", "gatt_notification"])
        XCTAssertEqual(try Data(contentsOf: directory.appendingPathComponent("pending.tsv")), pending)
        let backup = directory.appendingPathComponent("rejected/" + rejected.batchId + ".json")
        XCTAssertEqual(try JSONDecoder().decode(UploadBatch.self, from: Data(contentsOf: backup)), rejected)
        XCTAssertFalse(try store.recoverRejectedBatch(rejected))
        XCTAssertFalse(try store.recoverRejectedBatch(repaired))
        let reopened = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(try reopened.nextBatch(), repaired)
        XCTAssertEqual(repaired.bodySha256, Hashing.sha256Hex(Array(repaired.body.utf8)))
        try reopened.acknowledge(repaired, committedThrough: repaired.lastSourceSeq)
        let acknowledged = try RecordStore(directory: directory, installationId: installation)
        XCTAssertNil(try acknowledged.nextBatch())
        XCTAssertEqual(acknowledged.lastAcknowledged, 2)
    }

    func testOtherRejectionsLeaveTheOriginalBatchUntouched() throws {
        let (store, batch) = try legacyBatch([record(packet(100))])
        XCTAssertFalse(try store.recoverRejectedBatch(batch))
        XCTAssertEqual(try store.nextBatch(), batch)
    }

    func testBackupFailureLeavesOriginalRetryable() throws {
        let (store, batch) = try legacyBatch([record(packet(UInt64.max))])
        try Data().write(to: directory.appendingPathComponent("rejected"))
        XCTAssertThrowsError(try store.recoverRejectedBatch(batch))
        XCTAssertEqual(try store.nextBatch(), batch)
    }

    func testNewPacketsStayWithinBatchByteLimit() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        let context = CaptureContext(physicalDeviceId: nil)
        let bytes = packet(UInt64.max) + Array(repeating: UInt8(0), count: 2500)
        for _ in 0..<200 { store.append(context: context, record: record(bytes)) }
        var acknowledged = 0
        while let batch = try store.nextBatch() {
            XCTAssertLessThanOrEqual(batch.encodedBytes, RecordStore.maxBatchBytes)
            XCTAssertTrue(try records(batch).allSatisfy { $0["kind"] as? String == "collector_event" })
            acknowledged += batch.recordCount
            try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
        }
        XCTAssertEqual(acknowledged, 200)
    }
}
