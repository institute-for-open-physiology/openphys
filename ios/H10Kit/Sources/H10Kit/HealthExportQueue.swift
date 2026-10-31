import Foundation

public struct HealthReading: Codable, Equatable {
    public let id: String
    public let time: Date
    public let bpm: Int
    public let serial: String?
}

/// Pending Health writes retain their IDs across retries and app restarts.
public final class HealthExportQueue {
    public private(set) var readings: [HealthReading]
    private let url: URL
    private let limit: Int

    public init(url: URL, limit: Int = 3600) throws {
        self.url = url
        self.limit = limit
        readings = FileManager.default.fileExists(atPath: url.path)
            ? try JSONDecoder().decode([HealthReading].self, from: Data(contentsOf: url)) : []
    }

    public func append(_ sample: H10Protocol.HeartRateSample, at time: Date, serial: String?) throws {
        guard sample.hasHeartRate, time.timeIntervalSince1970.isFinite, time.timeIntervalSince1970 > 0 else { return }
        guard readings.count < limit else { throw H10Error("Health export is full. Turn it on again to retry.") }
        try save(readings + [HealthReading(id: Ids.new(), time: time, bpm: sample.bpm, serial: serial)])
    }

    public func acknowledge(_ ids: Set<String>) throws {
        try save(readings.filter { !ids.contains($0.id) })
    }

    private func save(_ next: [HealthReading]) throws {
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try JSONEncoder().encode(next).write(to: url, options: .atomic)
        readings = next
    }
}
