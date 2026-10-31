import Foundation
import XCTest
@testable import H10Kit

final class SharedSensorTests: XCTestCase {
    struct Fixtures: Decodable {
        struct Heart: Decodable {
            let name: String; let bytes: [UInt8]; let bpm: Int?
            let supported: Bool?; let detected: Bool?; let rr: [Double]?
            let age: Double?; let contact: Bool?
        }
        struct Measurement: Decodable {
            let name: String; let bytes: [UInt8]; let valid: Bool
            let ecg: [Int32]?; let acc: [[Int32]]?
        }
        struct Beats: Decodable { let rate: Double; let samples: [Float]; let expected: [Int] }
        let heartRate: [Heart]; let pmd: [Measurement]; let beats: Beats
    }

    func testSharedPackets() throws {
        let directory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent().appendingPathComponent("shared/fixtures/sensor.json")
        let fixtures = try JSONDecoder().decode(Fixtures.self, from: Data(contentsOf: directory))
        var detector = BeatDetector(rate: fixtures.beats.rate)
        let found = fixtures.beats.samples.enumerated().compactMap { detector.push($1, index: $0) }.filter { $0 > 320 }
        XCTAssertEqual(found.count, fixtures.beats.expected.count)
        for (a, b) in zip(found, fixtures.beats.expected) { XCTAssertLessThanOrEqual(abs(a - b), 1) }
        for row in fixtures.heartRate {
            let sample = H10Protocol.parseHeartRate(row.bytes)
            XCTAssertEqual(sample?.bpm, row.bpm, row.name)
            if let sample {
                XCTAssertEqual(sample.contactSupported, row.supported, row.name)
                XCTAssertEqual(sample.contactDetected, row.detected, row.name)
                XCTAssertEqual(sample.rrMs, row.rr, row.name)
                XCTAssertEqual(sample.contact(after: row.age!), row.contact, row.name)
            }
        }
        for row in fixtures.pmd {
            let frame = PMD.frame(row.bytes)
            XCTAssertEqual(frame != nil, row.valid, row.name)
            if let frame {
                XCTAssertEqual(PMD.ecgMicrovolts(frame), row.ecg, row.name)
                XCTAssertEqual(PMD.accMilliG(frame), row.acc, row.name)
            }
        }
    }
}
