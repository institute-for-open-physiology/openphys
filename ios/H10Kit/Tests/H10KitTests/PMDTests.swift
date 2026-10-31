import Foundation
import XCTest
@testable import H10Kit

// Fixtures shared with src/pmd.rs.
final class PMDTests: XCTestCase {
    private func bytes(_ hex: String) -> [UInt8] {
        var out: [UInt8] = []
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            out.append(UInt8(hex[index..<next], radix: 16)!)
            index = next
        }
        return out
    }

    func testLiveEcgFrame() throws {
        let frame = try XCTUnwrap(PMD.frame(bytes("005e2d107f63ff8c0b003401002b01003b01004201004001004e01005a0100630100740100870100a30100bd0100cb0100de0100f30100fa0100f10100d90100b601009001006601002b0100e60000a700006c00003300000b0000f0ffffd8ffffc8ffffc3ffffc6ffffcaffffd1ffffd8ffffe4fffff5ffff0400001500002000002000002800003300003600003100003300003300002c00002500001c00001900002800003f0000650000960000ae0000b00000a000007100004600003a0000250000090000040000070000f9fffff7ffff0000000700000b0000020000e4ffffcfffff")))
        XCTAssertEqual(frame.measurementType, PMD.ecg)
        let samples = try XCTUnwrap(PMD.ecgMicrovolts(frame))
        XCTAssertEqual(samples.count, 73)
        XCTAssertEqual(samples.first, 308)
        XCTAssertEqual(samples[72], -49)
    }

    func testLiveRawAccFrame() throws {
        let frame = try XCTUnwrap(PMD.frame(bytes("02d4ff7e7763ff8c0b0141fc5b00f30037fc6000ec0037fc5f00f10035fc6400f20035fc6500f60031fc6700f7002ffc6b00f9002dfc6e00f6002cfc7200f3002dfc7500f2002ffc7500f4002dfc7700f1002dfc7400f1002dfc7300f1002cfc7200ec002dfc7200ed002cfc7000f0002ffc6f00f3002efc6500f3002ffc6800f5002cfc6500f4002efc6200f7002efc6000f6002dfc5d00f4002bfc5d00f6002cfc6000f3002cfc6000f3002dfc6000f30030fc6000f30031fc5f00f50031fc6000f40031fc6000f30032fc6400f50031fc6300f90033fc6200fa0031fc6100fa00")))
        XCTAssertEqual(frame.frameType, 1)
        let samples = try XCTUnwrap(PMD.accMilliG(frame))
        XCTAssertEqual(samples.count, 36)
        XCTAssertEqual(samples.first, [-959, 91, 243])
        XCTAssertEqual(samples.last, [-975, 97, 250])
    }

    func testAccDeltaFrame() {
        let samples = PMD.deltaSamples([100, 0, 206, 255, 25, 0, 4, 2, 0xf1, 0xe0, 0x32], channels: 3, resolutionBits: 16)
        XCTAssertEqual(samples, [[100, -50, 25], [101, -51, 25], [99, -49, 28]])
    }
}

final class BeatDetectorTests: XCTestCase {
    private func ecg(beats: [Double], seconds: Double, polarity: Double) -> [Float] {
        (0..<Int(seconds * 130)).map { i in
            let t = Double(i) / 130
            var value = 60 * sin(2 * .pi * 0.3 * t)
            for beat in beats where abs(t - beat) < 0.6 {
                let d = t - beat
                value += polarity * 1100 * exp(-d * d / (2 * 0.011 * 0.011))
                value += 280 * exp(-(d - 0.28) * (d - 0.28) / (2 * 0.045 * 0.045))
                value += 120 * exp(-(d + 0.16) * (d + 0.16) / (2 * 0.025 * 0.025))
            }
            return Float(value)
        }
    }

    func testFindsEachRPeakAfterWarmupForEitherStrapOrientation() {
        let beats = stride(from: 0.5, to: 20, by: 0.8).map { $0 + 0.05 * sin($0) }
        let expected = beats.filter { $0 > 2.5 }.map { Int(($0 * 130).rounded()) }
        for polarity in [1.0, -1.0] {
            var detector = BeatDetector(rate: 130)
            let found = ecg(beats: beats, seconds: 20.5, polarity: polarity).enumerated()
                .compactMap { detector.push($1, index: $0) }
                .filter { $0 > Int(2.5 * 130) - 5 }
            XCTAssertEqual(found.count, expected.count, "polarity \(polarity)")
            for (peak, truth) in zip(found, expected) { XCTAssertLessThanOrEqual(abs(peak - truth), 1) }
        }
    }
}
