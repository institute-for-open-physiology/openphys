import Foundation
import XCTest
@testable import H10Kit

final class QueueCompressionTests: XCTestCase {
    private var directory: URL!
    private let installation = Ids.new()

    override func setUp() { directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString) }
    override func tearDown() { try? FileManager.default.removeItem(at: directory) }

    private func record(_ index: Int) -> RawRecord {
        RawRecord(connectionId: nil, collectorUnixNs: 1_790_000_000_000_000_000 + Int64(index), collectorMonotonicNs: UInt64(index),
                  physicalDeviceId: "h10_test", blePeripheralId: nil, kind: "collector_event",
                  body: ["event": "test", "severity": "info", "detail": "A repeated event with changing time and identity", "fields": ["index": .num(index)]])
    }

    func testCompressionPreservesEveryByteAcrossDictionaryResets() throws {
        let encoder = QueueCompression.Encoder(), decoder = QueueCompression.Decoder()
        var compressed = 0, raw = 0, groups = 0
        for index in 0..<3000 {
            let data = Data(RecordJSON.encode(sourceSeq: Int64(index), record: record(index)).utf8)
            let frame = try encoder.encode(data)
            let header = try QueueCompression.readHeader(frame)
            if header.reset { groups += 1 }
            XCTAssertEqual(try decoder.decode(Data(frame.dropFirst(QueueCompression.headerBytes)), header: header), data)
            compressed += frame.count
            raw += data.count
        }
        XCTAssertGreaterThan(groups, 2)
        XCTAssertLessThan(compressed, raw / 2)
    }

    func testLegacyQueueAndExactRetrySurviveCompressedAppends() throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let context = CaptureContext(physicalDeviceId: nil)
        let first = RecordJSON.encode(sourceSeq: 0, record: record(0))
        let path = directory.appendingPathComponent("pending.tsv")
        try Data("\(context.sessionId)\t\(context.epochId)\t0\t\(first)\n".utf8).write(to: path)
        var store = try RecordStore(directory: directory, installationId: installation)
        let original = try XCTUnwrap(store.nextBatch())
        let retry = try Data(contentsOf: directory.appendingPathComponent("batch.json"))
        for index in 1...700 { store.append(context: context, record: record(index)) }
        try store.flush()
        store = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(try store.nextBatch(), original)
        XCTAssertEqual(try Data(contentsOf: directory.appendingPathComponent("batch.json")), retry)
        try store.acknowledge(original, committedThrough: 0)
        var next: Int64 = 1
        while let batch = try store.nextBatch() {
            XCTAssertEqual(batch.firstSourceSeq, next)
            next = batch.lastSourceSeq + 1
            try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
            store = try RecordStore(directory: directory, installationId: installation)
        }
        XCTAssertEqual(next, 701)
    }

    func testCorruptFrameHeaderOrPayloadNeverDeletesSavedRecords() throws {
        let encoder = QueueCompression.Encoder()
        let frame = try encoder.encode(Data("session\tepoch\t0\t{\"ok\":true}\n".utf8))
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let path = directory.appendingPathComponent("pending.tsv")
        for index in [0, 1, 2, 3, 4, 8, 12, 16, QueueCompression.headerBytes + 2] {
            var corrupt = frame
            corrupt[index] ^= 1
            try corrupt.write(to: path)
            XCTAssertThrowsError(try RecordStore(directory: directory, installationId: installation))
            XCTAssertEqual(try Data(contentsOf: path), corrupt)
        }
    }

    func testNonemptyCompactionPreservesTheCompressedTailAndLaterAppends() throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let path = directory.appendingPathComponent("pending.tsv")
        FileManager.default.createFile(atPath: path.path, contents: nil)
        let handle = try FileHandle(forWritingTo: path)
        let context = CaptureContext(physicalDeviceId: nil)
        var expected: [String] = [], received: [String] = []
        for index in 0..<1400 {
            let row = RawRecord(connectionId: nil, collectorUnixNs: Int64(index), collectorMonotonicNs: UInt64(index),
                                physicalDeviceId: nil, blePeripheralId: nil, kind: "collector_event",
                                body: ["event": "test", "detail": .string(String(repeating: "x", count: 4096))])
            expected.append(row.recordId)
            let json = RecordJSON.encode(sourceSeq: Int64(index), record: row)
            try handle.write(contentsOf: Data("\(context.sessionId)\t\(context.epochId)\t\(index)\t\(json)\n".utf8))
        }
        try handle.close()
        var store = try RecordStore(directory: directory, installationId: installation)
        for index in 1400..<1500 {
            let row = record(index)
            expected.append(row.recordId)
            store.append(context: context, record: row)
        }
        var compacted = false
        while let batch = try store.nextBatch() {
            let json = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(batch.body.utf8)) as? [String: Any])
            let rows = try XCTUnwrap(json["records"] as? [[String: Any]])
            received += rows.compactMap { $0["record_id"] as? String }
            try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
            if !compacted, store.backlog().records > 0, try Data(contentsOf: path).first == 72 {
                compacted = true
                let row = record(1500)
                expected.append(row.recordId)
                store.append(context: context, record: row)
                try store.flush()
                store = try RecordStore(directory: directory, installationId: installation)
            }
        }
        XCTAssertTrue(compacted)
        XCTAssertEqual(received, expected)
        XCTAssertEqual(store.lastAcknowledged, 1500)
    }

    func testEveryTruncatedFrameRecoversAtThePreviousRecord() throws {
        let encoder = QueueCompression.Encoder()
        let first = try encoder.encode(Data("session\tepoch\t0\t{\"ok\":true}\n".utf8))
        let second = try encoder.encode(Data("session\tepoch\t1\t{\"ok\":false}\n".utf8))
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let path = directory.appendingPathComponent("pending.tsv")
        for length in 1..<second.count {
            try (first + second.prefix(length)).write(to: path)
            let store = try RecordStore(directory: directory, installationId: installation)
            XCTAssertEqual(store.backlog().records, 1)
            XCTAssertEqual(try Data(contentsOf: path), first)
            store.append(context: CaptureContext(physicalDeviceId: nil), record: record(1))
            try store.flush()
            XCTAssertEqual(try RecordStore(directory: directory, installationId: installation).backlog().records, 2)
        }
    }
}
