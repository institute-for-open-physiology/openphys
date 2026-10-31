import Foundation

/// Polar Measurement Data frames, decoded like the Rust collector's src/pmd.rs.
public enum PMD {
    public static let ecg: UInt8 = 0
    public static let acc: UInt8 = 2
    /// H10 frame timestamps count nanoseconds since 2000-01-01 in the sensor's local time.
    public static let epoch2000UnixNs: Int64 = 946_684_800_000_000_000
    /// Matches the cloud ingest limit for H10 local timestamps.
    public static let maximumTimestampNs: UInt64 = 150 * 366 * 24 * 60 * 60 * 1_000_000_000

    public struct Frame: Equatable {
        public let measurementType: UInt8
        public let timestampNs: UInt64
        public let frameType: UInt8
        public let compressed: Bool
        public let content: [UInt8]
    }

    public static func frame(_ data: [UInt8]) -> Frame? {
        guard data.count >= 10 else { return nil }
        var timestamp: UInt64 = 0
        for i in 0..<8 { timestamp |= UInt64(data[1 + i]) << (8 * UInt64(i)) }
        guard timestamp <= maximumTimestampNs else { return nil }
        return Frame(measurementType: data[0] & 0x3f, timestampNs: timestamp, frameType: data[9] & 0x7f,
                     compressed: data[9] & 0x80 != 0, content: Array(data[10...]))
    }

    /// ECG samples in microvolts, or nil for frames other than uncompressed type 0.
    public static func ecgMicrovolts(_ frame: Frame) -> [Int32]? {
        guard frame.measurementType == ecg, !frame.compressed, frame.frameType == 0 else { return nil }
        let content = frame.content
        return stride(from: 0, to: content.count - 2, by: 3).map { signExtend(content[$0..<$0 + 3]) }
    }

    /// Accelerometer samples in milli-g, each as [x, y, z].
    public static func accMilliG(_ frame: Frame) -> [[Int32]]? {
        guard frame.measurementType == acc else { return nil }
        if frame.compressed {
            guard frame.frameType <= 1, let samples = deltaSamples(frame.content, channels: 3, resolutionBits: 16) else { return nil }
            // Compressed type 0 arrives in g.
            return frame.frameType == 0 ? samples.map { $0.map { $0 &* 1000 } } : samples
        }
        guard frame.frameType <= 2 else { return nil }
        let width = Int(frame.frameType) + 1
        let size = width * 3
        let content = frame.content
        return stride(from: 0, to: content.count - size + 1, by: size).map { start in
            (0..<3).map { axis in signExtend(content[(start + axis * width)..<(start + (axis + 1) * width)]) }
        }
    }

    static func deltaSamples(_ data: [UInt8], channels: Int, resolutionBits: Int) -> [[Int32]]? {
        let width = (resolutionBits + 7) / 8
        guard resolutionBits > 0, data.count >= channels * width else { return nil }
        var current = (0..<channels).map { signExtend(data[($0 * width)..<(($0 + 1) * width)]) }
        var samples = [current]
        var offset = channels * width
        while offset + 2 <= data.count {
            let bits = Int(data[offset]), count = Int(data[offset + 1])
            offset += 2
            if bits == 0 || count == 0 { continue }
            let length = (bits * count * channels + 7) / 8
            guard offset + length <= data.count else { return nil }
            let payload = Array(data[offset..<offset + length])
            offset += length
            for sample in 0..<count {
                current = (0..<channels).map { channel in
                    current[channel] &+ signedBits(payload, at: (sample * channels + channel) * bits, width: bits)
                }
                samples.append(current)
            }
        }
        return samples
    }

    static func signExtend(_ bytes: ArraySlice<UInt8>) -> Int32 {
        var value: Int32 = 0
        for (i, byte) in bytes.enumerated() { value |= Int32(byte) << Int32(8 * i) }
        let bits = bytes.count * 8
        if bits > 0, bits < 32, value & (1 << Int32(bits - 1)) != 0 { value |= ~Int32(0) << Int32(bits) }
        return value
    }

    static func signedBits(_ payload: [UInt8], at offset: Int, width: Int) -> Int32 {
        var value: Int32 = 0
        for i in 0..<width {
            let bit = offset + i
            value |= Int32((payload[bit / 8] >> UInt8(bit % 8)) & 1) << Int32(i)
        }
        if width < 32, value & (1 << Int32(width - 1)) != 0 { value |= ~Int32(0) << Int32(width) }
        return value
    }
}
