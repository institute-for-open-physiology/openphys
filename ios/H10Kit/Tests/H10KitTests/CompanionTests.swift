import XCTest
@testable import H10Kit

final class CompanionTests: XCTestCase {
    private let now = Date(timeIntervalSince1970: 1_800_000_000)

    func testHeartRateExpiresEvenWhenSnapshotIsNew() {
        let fresh = WatchSnapshot(sentAt: now, measuredAt: now, heartRate: 72, connected: true, contact: true, battery: 90)
        XCTAssertEqual(fresh.currentHeartRate(at: now), 72)
        XCTAssertNil(fresh.currentHeartRate(at: now.addingTimeInterval(6)))
        let oldSample = WatchSnapshot(sentAt: now, measuredAt: now.addingTimeInterval(-6), heartRate: 72, connected: true, contact: true, battery: 90)
        XCTAssertNil(oldSample.currentHeartRate(at: now))
        let oldDelivery = WatchSnapshot(sentAt: now.addingTimeInterval(-6), measuredAt: now, heartRate: 72, connected: true, contact: true, battery: 90)
        XCTAssertNil(oldDelivery.currentHeartRate(at: now))
    }

    func testContactAndConnectionGateHeartRate() {
        for (connected, contact) in [(false, true), (true, false)] {
            let value = WatchSnapshot(sentAt: now, measuredAt: now, heartRate: 72, connected: connected, contact: contact, battery: nil)
            XCTAssertNil(value.currentHeartRate(at: now))
        }
        let future = WatchSnapshot(sentAt: now.addingTimeInterval(30), measuredAt: now, heartRate: 72, connected: true, contact: true, battery: nil)
        XCTAssertNil(future.currentHeartRate(at: now))
    }

    func testWatchRetryPreservesRenameAndCannotResurrectDeletion() throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).appendingPathComponent("marks.json")
        defer { try? FileManager.default.removeItem(at: url.deletingLastPathComponent()) }
        let mark = SensorMark(id: UUID().uuidString, time: now)
        var archive = MarkArchive()
        XCTAssertTrue(archive.receive(mark))
        archive.marks[0].label = "Stairs"
        try archive.write(to: url)
        archive = try JSONDecoder().decode(MarkArchive.self, from: Data(contentsOf: url))
        XCTAssertFalse(archive.receive(mark))
        XCTAssertEqual(archive.marks.map(\.label), ["Stairs"])
        archive.marks.removeAll()
        try archive.write(to: url)
        archive = try JSONDecoder().decode(MarkArchive.self, from: Data(contentsOf: url))
        XCTAssertFalse(archive.receive(mark))
        XCTAssertTrue(archive.marks.isEmpty)
    }

    func testDelayedWatchMarksKeepTapTimeAndSortChronologically() {
        var archive = MarkArchive(marks: [SensorMark(id: "phone", time: now)])
        let earlier = SensorMark(id: "watch", time: now.addingTimeInterval(-60))
        XCTAssertTrue(archive.receive(earlier))
        XCTAssertEqual(archive.marks.first, earlier)
    }

    func testQueueFlushPersistsMarkerBeforeReceipt() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try RecordStore(directory: directory, installationId: "phone")
        let context = CaptureContext(physicalDeviceId: nil)
        let record = RawRecord(connectionId: nil, collectorUnixNs: Clock.unixNs(now), collectorMonotonicNs: 0,
                               physicalDeviceId: nil, blePeripheralId: nil, kind: "collector_event", body: ["event": "manual_marker"])
        store.append(context: context, record: record)
        try store.flush()
        let reopened = try RecordStore(directory: directory, installationId: "phone")
        let batch = try XCTUnwrap(reopened.nextBatch())
        XCTAssertEqual(batch.recordCount, 1)
        XCTAssertTrue(batch.body.contains(record.recordId))
    }

    func testECGScaleHoldsAfterSpikeInsteadOfJumpingEachPacket() {
        var scale = ECGDisplayScale()
        scale.update(minimum: -1500, maximum: 2200, at: 1)
        XCTAssertEqual(scale.lower, -2000)
        XCTAssertEqual(scale.upper, 2500)
        scale.update(minimum: -100, maximum: 900, at: 2)
        XCTAssertEqual(scale.upper, 2500)
        scale.update(minimum: -100, maximum: 900, at: 12)
        XCTAssertEqual(scale.lower, -500)
        XCTAssertEqual(scale.upper, 1500)
        scale.update(minimum: .nan, maximum: .infinity, at: 30)
        XCTAssertEqual(scale.upper, 1500)
    }

    func testECGPreservesSamplesAndGapsThroughWatchTransport() throws {
        let ecg = WatchECG(measuredAt: now, offsets: [-1.0, -0.992, -0.2, -0.192], microvolts: [20, 1234, -210, 30], lower: -500, upper: 1500)
        let snapshot = WatchSnapshot(sentAt: now, measuredAt: now, heartRate: 72, connected: true, contact: true, battery: 90, ecg: ecg)
        let decoded = try JSONDecoder().decode(WatchSnapshot.self, from: JSONEncoder().encode(snapshot))
        XCTAssertEqual(decoded.currentECG(at: now), ecg)
        XCTAssertNil(decoded.currentECG(at: now.addingTimeInterval(3)))
        let resumed = WatchSnapshot(sentAt: now.addingTimeInterval(30), measuredAt: now, heartRate: 72, connected: true, contact: true, battery: 90, ecg: ecg)
        XCTAssertNil(resumed.currentECG(at: now.addingTimeInterval(30)))
        for (connected, contact) in [(false, true), (true, false)] {
            let lost = WatchSnapshot(sentAt: now, measuredAt: now, heartRate: 72, connected: connected, contact: contact, battery: 90, ecg: ecg)
            XCTAssertNil(lost.currentECG(at: now))
        }
    }

    func testWatchECGRejectsMalformedSampleArrays() {
        for (offsets, values) in [([-1.0, -0.9], [Int32(2)]), ([-1.0, -1.1], [2, 3]), ([-1.0, .nan], [2, 3])] {
            XCTAssertFalse(WatchECG(measuredAt: now, offsets: offsets, microvolts: values, lower: -500, upper: 1500).isValid)
        }
    }

    func testSixSecondECGFitsLiveMessageLimit() throws {
        let ecg = WatchECG(measuredAt: now, offsets: (0..<780).map { Double($0 - 780) / 130 }, microvolts: Array(repeating: Int32.min, count: 780), lower: -500, upper: 1500)
        let snapshot = WatchSnapshot(sentAt: now, measuredAt: now, heartRate: 72, connected: true, contact: true, battery: 90, ecg: ecg)
        XCTAssertLessThan(try JSONEncoder().encode(snapshot).count, 60_000)
    }
}
