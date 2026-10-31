import Foundation

public struct H10Error: Error, LocalizedError, CustomStringConvertible {
    public let message: String
    public init(_ message: String) { self.message = message }
    public var description: String { message }
    public var errorDescription: String? { message }
}

/// Polar H10 GATT/PMD/PS-FTP constants and codecs, ported from the Android collector.
public enum H10Protocol {
    public static let heartRateService = "0000180d-0000-1000-8000-00805f9b34fb"
    public static let heartRateMeasurement = "00002a37-0000-1000-8000-00805f9b34fb"
    public static let batteryLevel = "00002a19-0000-1000-8000-00805f9b34fb"
    public static let manufacturerName = "00002a29-0000-1000-8000-00805f9b34fb"
    public static let modelNumber = "00002a24-0000-1000-8000-00805f9b34fb"
    public static let serialNumber = "00002a25-0000-1000-8000-00805f9b34fb"
    public static let hardwareRevision = "00002a27-0000-1000-8000-00805f9b34fb"
    public static let firmwareRevision = "00002a26-0000-1000-8000-00805f9b34fb"
    public static let softwareRevision = "00002a28-0000-1000-8000-00805f9b34fb"
    public static let pmdControl = "fb005c81-02e7-f387-1cad-8acd2d8df0c8"
    public static let pmdData = "fb005c82-02e7-f387-1cad-8acd2d8df0c8"
    public static let psftpMtu = "fb005c51-02e7-f387-1cad-8acd2d8df0c8"

    public static let ecgStart: [UInt8] = [0x02, 0x00, 0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0e, 0x00]
    public static let accStart: [UInt8] = [0x02, 0x02, 0x00, 0x01, 0xc8, 0x00, 0x01, 0x01, 0x10, 0x00, 0x02, 0x01, 0x08, 0x00]
    public static let ecgStop: [UInt8] = [0x03, 0x00]
    public static let accStop: [UInt8] = [0x03, 0x02]

    /// Expands CoreBluetooth's 16/32-bit UUID strings to the lowercase 128-bit form the server expects.
    public static func fullUUID(_ uuidString: String) -> String {
        let value = uuidString.lowercased()
        switch value.count {
        case 4: return "0000" + value + "-0000-1000-8000-00805f9b34fb"
        case 8: return value + "-0000-1000-8000-00805f9b34fb"
        default: return value
        }
    }

    public static func describe(_ uuid: String) -> String {
        switch uuid {
        case pmdData: return "pmd_data"
        case pmdControl: return "pmd_control"
        case heartRateMeasurement: return "heart_rate_measurement"
        case batteryLevel: return "battery_level"
        case psftpMtu: return "psftp_mtu"
        default: return "unknown"
        }
    }

    public static func isMatchingControlResponse(_ payload: [UInt8], command: [UInt8]) -> Bool {
        payload.count >= 4 && command.count >= 2 && payload[0] == 0xf0 && payload[1] == command[0] && payload[2] == command[1]
    }

    public static func controlSucceeded(_ payload: [UInt8]) -> Bool { payload.count >= 4 && payload[3] == 0 }

    // MARK: PS-FTP local time

    public struct PsftpLocalTime {
        public let iso8601: String
        public let unixNs: Int64
        public let offsetMinutes: Int
        public let parameters: [UInt8]
        public let packets: [[UInt8]]
    }

    public static func psftpLocalTime(date: Date = Date(), timeZone: TimeZone = .current) -> PsftpLocalTime {
        let totalMs = Int64((date.timeIntervalSince1970 * 1000).rounded())
        let wholeSeconds = totalMs >= 0 ? totalMs / 1000 : (totalMs - 999) / 1000
        let millis = Int(totalMs - wholeSeconds * 1000)
        let secondDate = Date(timeIntervalSince1970: TimeInterval(wholeSeconds))
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = timeZone
        let parts = calendar.dateComponents([.year, .month, .day, .hour, .minute, .second], from: secondDate)
        let year = parts.year ?? 0, month = parts.month ?? 0, day = parts.day ?? 0
        let hour = parts.hour ?? 0, minute = parts.minute ?? 0, second = parts.second ?? 0
        let offsetMinutes = timeZone.secondsFromGMT(for: secondDate) / 60

        var dateFields: [UInt8] = []
        appendInt(1, year, to: &dateFields)
        appendInt(2, month, to: &dateFields)
        appendInt(3, day, to: &dateFields)
        var timeFields: [UInt8] = []
        appendInt(1, hour, to: &timeFields)
        appendInt(2, minute, to: &timeFields)
        appendInt(3, second, to: &timeFields)
        appendInt(4, millis, to: &timeFields)
        var parameters: [UInt8] = []
        appendMessage(1, dateFields, to: &parameters)
        appendMessage(2, timeFields, to: &parameters)
        appendInt(3, offsetMinutes, to: &parameters)

        let sign = offsetMinutes < 0 ? "-" : "+"
        let absOffset = abs(offsetMinutes)
        let iso = pad(year, 4) + "-" + pad(month, 2) + "-" + pad(day, 2) + "T" + pad(hour, 2) + ":" + pad(minute, 2) + ":" +
            pad(second, 2) + "." + pad(millis, 3) + sign + pad(absOffset / 60, 2) + ":" + pad(absOffset % 60, 2)
        return PsftpLocalTime(iso8601: iso, unixNs: totalMs * 1_000_000, offsetMinutes: offsetMinutes,
                              parameters: parameters, packets: psftpPackets(queryId: 3, parameters: parameters))
    }

    public static func psftpPackets(queryId: Int, parameters: [UInt8], mtuPayload: Int = 20) -> [[UInt8]] {
        precondition(mtuPayload >= 2)
        let stream: [UInt8] = [UInt8(queryId & 0xff), UInt8(((queryId >> 8) & 0xff) | 0x80)] + parameters
        var packets: [[UInt8]] = []
        var offset = 0
        var next = 0
        var sequence = 0
        repeat {
            let remaining = stream.count - offset
            let take = min(remaining, mtuPayload - 1)
            let status = remaining > mtuPayload - 1 ? 0x06 : 0x02
            packets.append([UInt8(status | next | (sequence << 4))] + stream[offset..<(offset + take)])
            offset += take
            next = 1
            sequence = (sequence + 1) & 0x0f
        } while offset < stream.count
        return packets
    }

    public struct PsftpResponseAssembler {
        private var expectedSequence = 0
        private var expectedNext = 0
        private var bytes: [UInt8] = []

        public init() {}

        /// Returns the full response once complete, nil while more frames are expected.
        public mutating func accept(_ frame: [UInt8]) throws -> [UInt8]? {
            guard let first = frame.first else { throw H10Error("empty PS-FTP response frame") }
            let header = Int(first)
            let next = header & 0x01
            let status = (header & 0x06) >> 1
            let sequence = header >> 4
            guard sequence == expectedSequence else { throw H10Error("PS-FTP sequence mismatch") }
            guard next == expectedNext else { throw H10Error("PS-FTP next-frame mismatch") }
            expectedSequence = (expectedSequence + 1) & 0x0f
            expectedNext = 1
            switch status {
            case 3:
                bytes += frame.dropFirst()
                return nil
            case 1:
                bytes += frame.dropFirst()
                return bytes
            case 0:
                if frame.count == 3 {
                    let error = Int(frame[1]) | (Int(frame[2]) << 8)
                    guard error == 0 else { throw H10Error("PS-FTP response error \(error)") }
                }
                return bytes
            default:
                throw H10Error("unexpected PS-FTP status \(status)")
            }
        }
    }

    // MARK: Heart rate

    public struct HeartRateSample: Equatable {
        public static let maximumAge = 2.5
        public let bpm: Int
        public let contactSupported: Bool
        public let contactDetected: Bool
        public let rrMs: [Double]

        public var hasHeartRate: Bool { bpm > 0 && (!contactSupported || contactDetected) }

        public func contact(after seconds: Double) -> Bool? {
            guard seconds >= 0, seconds < Self.maximumAge, contactSupported else { return nil }
            if !contactDetected { return false }
            return hasHeartRate ? true : nil
        }
    }

    public static func parseHeartRate(_ data: [UInt8]) -> HeartRateSample? {
        guard data.count >= 2 else { return nil }
        let flags = data[0]
        var index: Int
        let bpm: Int
        if flags & 0x01 != 0 {
            guard data.count >= 3 else { return nil }
            bpm = Int(data[1]) | (Int(data[2]) << 8)
            index = 3
        } else {
            bpm = Int(data[1])
            index = 2
        }
        if flags & 0x08 != 0 { index += 2 }
        var rr: [Double] = []
        if flags & 0x10 != 0 {
            while index + 1 < data.count {
                rr.append(Double(Int(data[index]) | (Int(data[index + 1]) << 8)) * 1000 / 1024)
                index += 2
            }
        }
        return HeartRateSample(bpm: bpm, contactSupported: flags & 0x04 != 0, contactDetected: flags & 0x02 != 0, rrMs: rr)
    }

    private static func appendInt(_ field: Int, _ value: Int, to out: inout [UInt8]) {
        out.append(UInt8(field << 3))
        appendVarint(UInt64(bitPattern: Int64(value)), to: &out)
    }

    private static func appendMessage(_ field: Int, _ value: [UInt8], to out: inout [UInt8]) {
        out.append(UInt8((field << 3) | 2))
        appendVarint(UInt64(value.count), to: &out)
        out += value
    }

    private static func appendVarint(_ input: UInt64, to out: inout [UInt8]) {
        var value = input
        while value >= 0x80 {
            out.append(UInt8(value & 0x7f) | 0x80)
            value >>= 7
        }
        out.append(UInt8(value))
    }

    private static func pad(_ value: Int, _ width: Int) -> String {
        let text = String(value)
        return text.count >= width ? text : String(repeating: "0", count: width - text.count) + text
    }
}

public enum H10Identity {
    public static func matches(name: String?, configuredSerial: String?, peripheralId: String, savedPeripheralId: String?) -> Bool {
        if let serial = serial(fromAdvertisedName: name) { return configuredSerial == nil || configuredSerial == serial }
        return name == nil && configuredSerial != nil && peripheralId.lowercased() == savedPeripheralId?.lowercased()
    }

    public static func serial(fromAdvertisedName name: String?) -> String? {
        guard let name = name?.trimmingCharacters(in: .whitespaces) else { return nil }
        let prefix = "polar h10 "
        guard name.lowercased().hasPrefix(prefix) else { return nil }
        return normalizedSerial(String(name.dropFirst(prefix.count)))
    }

    public static func normalizedSerial(_ value: String?) -> String? {
        let trimSet = CharacterSet.whitespacesAndNewlines.union(CharacterSet(charactersIn: "\u{0}"))
        guard let serial = value?.trimmingCharacters(in: trimSet).uppercased(),
              (4...32).contains(serial.count),
              serial.unicodeScalars.allSatisfy({ (65...90).contains($0.value) || (48...57).contains($0.value) })
        else { return nil }
        return serial
    }

    public static func physicalDeviceId(serial: String) -> String { "h10_" + serial.lowercased() }
}

/// Enrollment secrets travel in the URL fragment: https://sensors-poc.vineyard-finance.com/enroll#TOKEN
public enum EnrollmentLink {
    public static let host = "sensors-poc.vineyard-finance.com"

    public static func isToken(_ value: String) -> Bool {
        value.utf8.count == 43 && value.unicodeScalars.allSatisfy { scalar in
            let v = scalar.value
            return (48...57).contains(v) || (65...90).contains(v) || (97...122).contains(v) || v == 45 || v == 95
        }
    }

    public static func token(from input: String, origin: URL = URL(string: "https://" + host)!) -> String? {
        let text = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if isToken(text) { return text }
        guard let parts = URLComponents(string: text),
              parts.scheme?.lowercased() == "https",
              parts.host?.lowercased() == origin.host?.lowercased(),
              parts.user == nil, parts.password == nil,
              (parts.port ?? 443) == (origin.port ?? 443),
              parts.percentEncodedPath == "/enroll",
              parts.percentEncodedQuery == nil,
              let fragment = parts.percentEncodedFragment, isToken(fragment)
        else { return nil }
        return fragment
    }
}
