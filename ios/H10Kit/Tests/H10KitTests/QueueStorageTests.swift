import Foundation
import XCTest
@testable import H10Kit
#if os(macOS)
import Darwin
#endif

final class QueueStorageTests: XCTestCase {
    private var directory: URL!
    private let installation = "8b6c2a8e-6a1f-4a6e-9c55-0d6b9f0f1a10"

    override func setUp() { directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString) }
    override func tearDown() { try? FileManager.default.removeItem(at: directory) }

    private func record(_ index: Int, padding: Int = 0) -> RawRecord {
        RawRecord(connectionId: nil, collectorUnixNs: 1_790_000_000_000_000_000 + Int64(index), collectorMonotonicNs: UInt64(index),
                  physicalDeviceId: nil, blePeripheralId: nil, kind: "collector_event",
                  body: ["event": "test", "severity": "info", "detail": .string(String(repeating: "x", count: padding)), "fields": ["index": .num(index)]])
    }

    func testFailedAckLeavesExactBatchAndRecordsRetryable() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        store.append(context: CaptureContext(physicalDeviceId: nil), record: record(0))
        let batch = try XCTUnwrap(store.nextBatch())
        let pending = try Data(contentsOf: directory.appendingPathComponent("pending.tsv"))
        let ack = directory.appendingPathComponent("last_ack")
        try FileManager.default.createDirectory(at: ack, withIntermediateDirectories: false)
        XCTAssertThrowsError(try store.acknowledge(batch, committedThrough: batch.lastSourceSeq))
        XCTAssertEqual(store.lastAcknowledged, -1)
        XCTAssertEqual(store.backlog().records, 1)
        XCTAssertEqual(try store.nextBatch(), batch)
        XCTAssertEqual(try Data(contentsOf: directory.appendingPathComponent("pending.tsv")), pending)
        try FileManager.default.removeItem(at: ack)
        try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
        XCTAssertEqual(store.backlog().records, 0)
    }

    func testCorruptCompleteRecordIsPreservedInsteadOfDropped() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        store.append(context: CaptureContext(physicalDeviceId: nil), record: record(0))
        try store.flush()
        let path = directory.appendingPathComponent("pending.tsv")
        let handle = try FileHandle(forWritingTo: path)
        try handle.seekToEnd()
        try handle.write(contentsOf: Data("broken\tcomplete\t1\t{bad\n".utf8))
        try handle.close()
        let before = try Data(contentsOf: path)
        XCTAssertThrowsError(try RecordStore(directory: directory, installationId: installation))
        XCTAssertEqual(try Data(contentsOf: path), before)
    }

    func testCorruptAckAndRetryBatchAreNotSilentlyReplaced() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        store.append(context: CaptureContext(physicalDeviceId: nil), record: record(0))
        _ = try store.nextBatch()
        let batch = directory.appendingPathComponent("batch.json")
        try Data("broken".utf8).write(to: batch)
        XCTAssertThrowsError(try store.nextBatch())
        XCTAssertEqual(try String(contentsOf: batch), "broken")
        try Data("broken".utf8).write(to: directory.appendingPathComponent("last_ack"))
        XCTAssertThrowsError(try RecordStore(directory: directory, installationId: installation))
    }

    func testCompactionFailureDoesNotBlockUploadsOrLaterAppends() throws {
        let store = try RecordStore(directory: directory, installationId: installation)
        let context = CaptureContext(physicalDeviceId: nil)
        store.append(context: context, record: record(0))
        let batch = try XCTUnwrap(store.nextBatch())
        let blocked = directory.appendingPathComponent("pending.compacting")
        try FileManager.default.createDirectory(at: blocked, withIntermediateDirectories: false)
        try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
        XCTAssertNil(store.lastFailure)
        store.append(context: context, record: record(1))
        try store.flush()
        let loaded = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(try loaded.nextBatch()?.firstSourceSeq, 1)
        XCTAssertEqual(loaded.backlog().records, 1)
    }

    func testLargeBacklogDrainsAcrossRestartWithoutLosingNewArrivals() throws {
        var store = try RecordStore(directory: directory, installationId: installation)
        let first = CaptureContext(physicalDeviceId: nil)
        for i in 0..<6000 { store.append(context: first, record: record(i, padding: 1024)) }
        let batch = try XCTUnwrap(store.nextBatch())
        let before = store.backlog()
        store = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(store.backlog(), before)
        XCTAssertEqual(try store.nextBatch(), batch)
        let second = CaptureContext(physicalDeviceId: nil)
        store.append(context: second, record: record(6000))
        var next: Int64 = 0
        while let current = try store.nextBatch() {
            XCTAssertEqual(current.firstSourceSeq, next)
            XCTAssertLessThanOrEqual(current.encodedBytes, RecordStore.maxBatchBytes)
            next = current.lastSourceSeq + 1
            try store.acknowledge(current, committedThrough: current.lastSourceSeq)
        }
        XCTAssertEqual(next, 6001)
        XCTAssertEqual(store.backlog(), Backlog(records: 0, bytes: 0))
        XCTAssertEqual(try Data(contentsOf: directory.appendingPathComponent("pending.tsv")).count, 0)
        store = try RecordStore(directory: directory, installationId: installation)
        store.append(context: second, record: record(6001))
        XCTAssertEqual(try store.nextBatch()?.firstSourceSeq, 6001)
    }

    func testPartialWriteFailureCanRecoverWithoutDamagingSavedRecords() throws {
        var fail = false
        let store = try RecordStore(directory: directory, installationId: installation) { handle, data in
            if fail {
                try handle.write(contentsOf: data.prefix(17))
                throw CocoaError(.fileWriteOutOfSpace)
            }
            try handle.write(contentsOf: data)
        }
        let context = CaptureContext(physicalDeviceId: nil)
        store.append(context: context, record: record(0))
        let batch = try XCTUnwrap(store.nextBatch())
        fail = true
        store.append(context: context, record: record(1))
        XCTAssertNotNil(store.lastFailure)
        XCTAssertEqual(store.backlog().records, 1)
        store.append(context: context, record: record(2))
        XCTAssertThrowsError(try store.flush())
        fail = false
        XCTAssertEqual(try store.resumeWrites(), 1)
        XCTAssertNil(store.lastFailure)
        XCTAssertEqual(try store.nextBatch(), batch)
        store.append(context: context, record: record(3))
        try store.flush()
        let reopened = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(reopened.backlog().records, 4)
        try reopened.acknowledge(batch, committedThrough: batch.lastSourceSeq)
        let next = try XCTUnwrap(reopened.nextBatch())
        XCTAssertEqual(next.firstSourceSeq, 1)
        XCTAssertTrue(next.body.contains("\"unsaved_records\":1"))
        XCTAssertTrue(next.body.contains("\"index\":1"))
        XCTAssertTrue(next.body.contains("\"index\":3"))
    }

    func testCompactionExcludesPartialFailedWrite() throws {
        var fail = false
        let store = try RecordStore(directory: directory, installationId: installation) { handle, data in
            if fail {
                try handle.write(contentsOf: data.prefix(17))
                throw CocoaError(.fileWriteOutOfSpace)
            }
            try handle.write(contentsOf: data)
        }
        let context = CaptureContext(physicalDeviceId: nil)
        store.append(context: context, record: record(0))
        let batch = try XCTUnwrap(store.nextBatch())
        fail = true
        store.append(context: context, record: record(1))
        XCTAssertNotNil(store.lastFailure)
        try store.acknowledge(batch, committedThrough: batch.lastSourceSeq)
        fail = false
        XCTAssertEqual(try store.resumeWrites(), 0)
        store.append(context: context, record: record(2))
        try store.flush()
        let reopened = try RecordStore(directory: directory, installationId: installation)
        XCTAssertEqual(reopened.backlog().records, 3)
        XCTAssertTrue(try XCTUnwrap(reopened.nextBatch()).body.contains("\"index\":2"))
    }

    func testRepeatedStorageFailuresRetainTheLossCountUntilRecoveryIsSaved() throws {
        var fail = true
        let store = try RecordStore(directory: directory, installationId: installation) { handle, data in
            if fail { throw CocoaError(.fileWriteOutOfSpace) }
            try handle.write(contentsOf: data)
        }
        let context = CaptureContext(physicalDeviceId: nil)
        store.append(context: context, record: record(0))
        store.append(context: context, record: record(1))
        XCTAssertThrowsError(try store.resumeWrites())
        XCTAssertNotNil(store.lastFailure)
        store.append(context: context, record: record(2))
        XCTAssertThrowsError(try store.resumeWrites())
        fail = false
        XCTAssertEqual(try store.resumeWrites(), 2)
        XCTAssertNil(store.lastFailure)
        let reopened = try RecordStore(directory: directory, installationId: installation)
        let batch = try XCTUnwrap(reopened.nextBatch())
        XCTAssertEqual(batch.recordCount, 2)
        XCTAssertTrue(batch.body.contains("\"unsaved_records\":2"))
        XCTAssertTrue(batch.body.contains("\"index\":0"))
        let json = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(batch.body.utf8)) as? [String: Any])
        let records = try XCTUnwrap(json["records"] as? [[String: Any]])
        let elapsed = try XCTUnwrap(records.last?["collector_monotonic_ns"] as? UInt64)
        XCTAssertLessThanOrEqual(elapsed, Clock.monotonicNs() - context.epochStartMonotonicNs)
    }

    #if os(macOS)
    private func memoryBytes() -> UInt64 {
        var info = task_vm_info()
        var count = mach_msg_type_number_t(MemoryLayout<task_vm_info>.size / MemoryLayout<natural_t>.size)
        let result = withUnsafeMutablePointer(to: &info) { pointer in
            pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
            }
        }
        XCTAssertEqual(result, KERN_SUCCESS)
        return info.phys_footprint
    }

    func testOpeningLargeOfflineQueueUsesBoundedMemory() throws {
        try measureQueueMemory(compressed: false)
    }

    func testOpeningLargeCompressedQueueUsesBoundedMemory() throws {
        try measureQueueMemory(compressed: true)
    }

    private func measureQueueMemory(compressed: Bool) throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent("pending.tsv")
        let context = CaptureContext(physicalDeviceId: nil)
        let row = record(0, padding: 4096)
        FileManager.default.createFile(atPath: url.path, contents: nil)
        let handle = try FileHandle(forWritingTo: url)
        let encoder = QueueCompression.Encoder()
        let count = Int(ProcessInfo.processInfo.environment["H10_QUEUE_STRESS_RECORDS"] ?? "30000") ?? 30_000
        for i in 0..<count {
            try autoreleasepool {
                let json = RecordJSON.encode(sourceSeq: Int64(i), record: row)
                let line = "\(context.sessionId)\t\(context.epochId)\t\(i)\t\(json)\n"
                let data = Data(line.utf8)
                try handle.write(contentsOf: compressed ? encoder.encode(data) : data)
            }
        }
        let diskBytes = try handle.seekToEnd()
        try handle.close()
        let before = memoryBytes()
        let started = Date()
        let store = try RecordStore(directory: directory, installationId: installation)
        let batch = try XCTUnwrap(store.nextBatch())
        let after = memoryBytes()
        XCTAssertEqual(store.backlog().records, count)
        XCTAssertEqual(batch.firstSourceSeq, 0)
        XCTAssertLessThanOrEqual(batch.encodedBytes, RecordStore.maxBatchBytes)
        let growth = Int64(after) - Int64(before)
        print("Offline queue: \(diskBytes) disk bytes; \(store.backlog().bytes) JSON bytes; memory growth \(growth) bytes; open+batch \(Date().timeIntervalSince(started)) seconds")
        XCTAssertLessThan(growth, 32 * 1024 * 1024, "Opening a backlog must not load all its records into RAM")
    }
    #endif
}
