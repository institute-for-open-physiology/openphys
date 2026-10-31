import Foundation

public struct WatchSnapshot: Codable, Equatable {
    public let sentAt: Date
    public let measuredAt: Date?
    public let heartRate: Int?
    public let connected: Bool
    public let contact: Bool?
    public let battery: Int?
    public let ecg: WatchECG?

    public init(sentAt: Date, measuredAt: Date?, heartRate: Int?, connected: Bool, contact: Bool?, battery: Int?, ecg: WatchECG? = nil) {
        self.sentAt = sentAt; self.measuredAt = measuredAt; self.heartRate = heartRate
        self.connected = connected; self.contact = contact; self.battery = battery
        self.ecg = ecg
    }

    public func isCurrent(at now: Date) -> Bool { (-2...5).contains(now.timeIntervalSince(sentAt)) }
    public func currentHeartRate(at now: Date) -> Int? {
        guard isCurrent(at: now), connected, contact != false, let measuredAt,
              (-2...5).contains(now.timeIntervalSince(measuredAt)), let heartRate, heartRate > 0 else { return nil }
        return heartRate
    }

    public func currentECG(at now: Date) -> WatchECG? {
        guard isCurrent(at: now), connected, contact != false, let ecg,
              (-2...2.5).contains(now.timeIntervalSince(ecg.measuredAt)), ecg.isValid else { return nil }
        return ecg
    }
}

public struct WatchECG: Codable, Equatable {
    public let measuredAt: Date
    /// Sample times relative to the snapshot's sentAt, retaining gaps between packets.
    public let offsets: [Double]
    public let microvolts: [Int32]
    public let lower: Double
    public let upper: Double

    public init(measuredAt: Date, offsets: [Double], microvolts: [Int32], lower: Double, upper: Double) {
        self.measuredAt = measuredAt; self.offsets = offsets; self.microvolts = microvolts
        self.lower = lower; self.upper = upper
    }

    public var isValid: Bool {
        (2...1024).contains(offsets.count) && offsets.count == microvolts.count
        && lower.isFinite && upper.isFinite && upper > lower
        && offsets.allSatisfy { $0.isFinite && (-8...0.5).contains($0) }
        && zip(offsets, offsets.dropFirst()).allSatisfy { $0 < $1 }
    }
}

public struct SensorMark: Codable, Identifiable, Equatable {
    public let id: String
    public let time: Date
    public var label: String
    public var displayLabel: String { label == "Mark" ? "Event" : label }
    public init(id: String, time: Date, label: String = "Event") { self.id = id; self.time = time; self.label = label }
}

/// Marks and Watch receipts share one atomic file, so retries cannot resurrect deleted marks.
public struct MarkArchive: Codable {
    public var marks: [SensorMark] = []
    private var watchReceipts: Set<String> = []
    public init(marks: [SensorMark] = []) { self.marks = marks }

    @discardableResult
    public mutating func receive(_ mark: SensorMark) -> Bool {
        guard watchReceipts.insert(mark.id).inserted else { return false }
        if !marks.contains(where: { $0.id == mark.id }) { marks.append(mark); marks.sort { $0.time < $1.time } }
        return true
    }

    public func write(to url: URL) throws {
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try JSONEncoder().encode(self).write(to: url, options: .atomic)
    }
}

public struct ECGDisplayScale {
    public private(set) var lower = -500.0
    public private(set) var upper = 1500.0
    private var heldUntil = -Double.infinity
    public init() {}
    public mutating func update(minimum: Double, maximum: Double, at time: Double) {
        guard minimum.isFinite, maximum.isFinite else { return }
        let low = min(-500, floor((minimum - 100) / 500) * 500)
        let high = max(1500, ceil((maximum + 100) / 500) * 500)
        if low < lower || high > upper {
            lower = min(lower, low); upper = max(upper, high); heldUntil = time + 10
        } else if time >= heldUntil {
            lower = low; upper = high; heldUntil = time + 10
        }
    }
}
