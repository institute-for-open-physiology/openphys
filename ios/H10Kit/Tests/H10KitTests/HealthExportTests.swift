import XCTest
@testable import H10Kit

final class HealthExportTests: XCTestCase {
    private var directory: URL!
    private var url: URL { directory.appendingPathComponent("health.json") }
    private let time = Date(timeIntervalSince1970: 1_800_000_000)

    override func setUp() {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    }

    override func tearDown() { try? FileManager.default.removeItem(at: directory) }

    func testRetryKeepsIdentityAndAcknowledgesOnlySavedReadings() throws {
        let queue = try HealthExportQueue(url: url)
        let sample = try XCTUnwrap(H10Protocol.parseHeartRate([0x06, 72]))
        try queue.append(sample, at: time, serial: "sensor")
        let pending = queue.readings
        XCTAssertEqual(try HealthExportQueue(url: url).readings, pending)
        try queue.append(sample, at: time.addingTimeInterval(1), serial: "sensor")
        let second = try XCTUnwrap(queue.readings.last)
        try queue.acknowledge(Set(pending.map(\.id)))
        XCTAssertEqual(try HealthExportQueue(url: url).readings, [second])
        try queue.acknowledge(Set(pending.map(\.id)))
        XCTAssertEqual(queue.readings, [second])
        XCTAssertEqual(second.time, time.addingTimeInterval(1))
        XCTAssertEqual(second.serial, "sensor")
    }

    func testOffSkinAndZeroReadingsAreNotExported() throws {
        let queue = try HealthExportQueue(url: url)
        for bytes: [UInt8] in [[0x04, 72], [0x06, 0]] {
            try queue.append(XCTUnwrap(H10Protocol.parseHeartRate(bytes)), at: time, serial: nil)
        }
        XCTAssertTrue(queue.readings.isEmpty)
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    }

    func testFullQueuePreservesPendingReadings() throws {
        let queue = try HealthExportQueue(url: url, limit: 1)
        let sample = try XCTUnwrap(H10Protocol.parseHeartRate([0x06, 72]))
        try queue.append(sample, at: time, serial: nil)
        let pending = queue.readings
        XCTAssertThrowsError(try queue.append(sample, at: time.addingTimeInterval(1), serial: nil))
        XCTAssertEqual(queue.readings, pending)
        XCTAssertEqual(try HealthExportQueue(url: url).readings, pending)
    }

    func testUnreadableQueueIsNotOverwritten() throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let damaged = Data("incomplete".utf8)
        try damaged.write(to: url)
        XCTAssertThrowsError(try HealthExportQueue(url: url))
        XCTAssertEqual(try Data(contentsOf: url), damaged)
    }

    func testWriteFailureDoesNotAcknowledgePendingReadings() throws {
        let queue = try HealthExportQueue(url: url)
        try queue.append(XCTUnwrap(H10Protocol.parseHeartRate([0x06, 72])), at: time, serial: nil)
        let pending = queue.readings
        try FileManager.default.removeItem(at: url)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        XCTAssertThrowsError(try queue.acknowledge(Set(pending.map(\.id))))
        XCTAssertEqual(queue.readings, pending)
    }

    func testLegacyDefaultEventLabelPreservesStoredContent() {
        let event = SensorMark(id: "legacy", time: time, label: "Mark")
        XCTAssertEqual(event.displayLabel, "Event")
        XCTAssertEqual(event.label, "Mark")
        XCTAssertEqual(SensorMark(id: "custom", time: time, label: "Coffee").displayLabel, "Coffee")
    }
}
