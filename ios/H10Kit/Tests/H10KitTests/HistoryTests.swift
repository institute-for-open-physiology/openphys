import Foundation
import XCTest
@testable import H10Kit

final class HistoryTests: XCTestCase {
    private var directory: URL!
    private var time: Double!
    override func setUp() {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        time = (Date().timeIntervalSince1970 / 5).rounded(.down) * 5 - 30
    }
    override func tearDown() { try? FileManager.default.removeItem(at: directory) }

    func testAveragesKeepGapsAndTheActiveBin() {
        let history = History(directory: directory)
        history.add(.heartRate, 60, at: time)
        history.add(.heartRate, 80, at: time + 1)
        history.add(.rr, 800, at: time + 1)
        history.add(.heartRate, 90, at: time + 10)
        XCTAssertEqual(history.series(.heartRate, from: time, to: time + 15, count: 3), [70, nil, 90])
        XCTAssertEqual(history.series(.rr, from: time, to: time + 15, count: 3), [800, nil, nil])
    }

    func testFlushSurvivesRelaunch() {
        var history: History? = History(directory: directory)
        history?.add(.heartRate, 72, at: time)
        history?.flush()
        history = nil
        let reopened = History(directory: directory)
        XCTAssertEqual(reopened.series(.heartRate, from: time, to: time + 5, count: 1), [72])
    }

    func testBackgroundFlushAndRelaunchKeepTheWholeBin() {
        var history: History? = History(directory: directory)
        history?.add(.heartRate, 60, at: time)
        history?.add(.heartRate, 90, at: time + 1)
        history?.flush()
        history?.add(.rssi, -60, at: time + 2)
        history?.flush()
        history = nil
        let reopened = History(directory: directory)
        reopened.add(.heartRate, 90, at: time + 3)
        reopened.flush()
        XCTAssertEqual(reopened.series(.heartRate, from: time, to: time + 5, count: 1), [80])
        XCTAssertEqual(reopened.series(.rssi, from: time, to: time + 5, count: 1), [-60])
    }

    func testActivityIgnoresSteadyGravity() {
        let history = History(directory: directory)
        for _ in 0..<20 { history.addMotion(1000, at: time) }
        for index in 0..<20 { history.addMotion(index.isMultiple(of: 2) ? 900 : 1100, at: time + 5) }
        XCTAssertEqual(history.series(.activity, from: time, to: time + 10, count: 2), [0, 100])
    }

    func testTornRecordRecoveryKeepsLaterWritesReadable() throws {
        var history: History? = History(directory: directory)
        history?.add(.heartRate, 72, at: time)
        history?.flush()
        history = nil
        let url = try XCTUnwrap(FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil).first)
        let handle = try FileHandle(forWritingTo: url)
        try handle.seekToEnd()
        try handle.write(contentsOf: Data([1, 2, 3]))
        try handle.close()
        history = History(directory: directory)
        history?.add(.heartRate, 80, at: time + 5)
        history?.flush()
        history = nil
        let reopened = History(directory: directory)
        XCTAssertEqual(reopened.series(.heartRate, from: time, to: time + 10, count: 2), [72, 80])
    }

    func testRetentionRunsWhileTheAppStaysOpen() {
        let history = History(directory: directory)
        history.add(.heartRate, 60, at: time - 10 * 86_400)
        history.add(.heartRate, 70, at: time)
        history.flush()
        XCTAssertEqual(history.times, [time])
    }
}

final class RemoteEnrollmentTests: XCTestCase {
    func testEnrollmentMustMatchTheSelectedServerAndPort() {
        let origin = URL(string: "https://home-server.example:10443")!
        let token = String(repeating: "a", count: 43)
        XCTAssertEqual(EnrollmentLink.token(from: "https://home-server.example:10443/enroll#" + token, origin: origin), token)
        XCTAssertNil(EnrollmentLink.token(from: "https://home-server.example/enroll#" + token, origin: origin))
        XCTAssertNil(EnrollmentLink.token(from: "https://other.example:10443/enroll#" + token, origin: origin))
    }

    func testOneServerAckDoesNotDeleteAnotherServersQueue() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let a = try RecordStore(directory: directory.appendingPathComponent("a"), installationId: Ids.new())
        let b = try RecordStore(directory: directory.appendingPathComponent("b"), installationId: Ids.new())
        let context = CaptureContext(physicalDeviceId: nil)
        let record = RawRecord(connectionId: nil, collectorUnixNs: Clock.unixNs(), collectorMonotonicNs: 0,
                               physicalDeviceId: nil, blePeripheralId: nil, kind: "collector_event", body: [:])
        a.append(context: context, record: record)
        b.append(context: context, record: record)
        let batchA = try XCTUnwrap(a.nextBatch())
        let batchB = try XCTUnwrap(b.nextBatch())
        try a.acknowledge(batchA, committedThrough: batchA.lastSourceSeq)
        XCTAssertEqual(a.backlog().records, 0)
        XCTAssertEqual(b.backlog().records, 1)
        XCTAssertEqual(try b.nextBatch(), batchB)
    }
}
