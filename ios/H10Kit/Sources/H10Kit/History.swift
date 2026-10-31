import Foundation

/// Five-second averages of each signal, kept on disk for a week so History can show past days. Main thread only.
public final class History {
    public enum Metric: Int, CaseIterable { case heartRate, rr, activity, rssi, delay }

    public static let binSeconds = 5.0
    public static let keepDays = 8.0
    private static let recordBytes = 8 + 8 * Metric.allCases.count + 20
    private static let dayFormat: DateFormatter = {
        let format = DateFormatter()
        format.locale = Locale(identifier: "en_US_POSIX")
        format.dateFormat = "yyyy-MM-dd"
        return format
    }()

    public private(set) var times: [Double] = []
    private var columns = Array(repeating: [Float](), count: Metric.allCases.count)
    private var binStart: Double?
    private var sums = Array(repeating: 0.0, count: Metric.allCases.count)
    private var counts = Array(repeating: 0, count: Metric.allCases.count)
    private var motion = (sum: 0.0, squares: 0.0, count: 0)
    public var persistent = true
    public private(set) var failure: String?
    private let directory: URL
    private var file: (day: String, handle: FileHandle)?

    public var earliest: Double? { times.first }

    public init(directory: URL) {
        self.directory = directory
        do { try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true) }
        catch { failure = "History could not be opened." }
        load()
    }

    deinit { try? file?.handle.close() }

    public func add(_ metric: Metric, _ value: Double, at time: Double = Date().timeIntervalSince1970) {
        guard value.isFinite, time.isFinite else { return }
        roll(to: time)
        sums[metric.rawValue] += value
        counts[metric.rawValue] += 1
    }

    /// Activity is the spread of the acceleration magnitude within a bin, in mG.
    public func addMotion(_ magnitude: Double, at time: Double) {
        guard magnitude.isFinite, time.isFinite else { return }
        roll(to: time)
        motion.sum += magnitude
        motion.squares += magnitude * magnitude
        motion.count += 1
    }

    /// Averages each of count equal slices of the time range; nil where a slice has no data.
    public func series(_ metric: Metric, from start: Double, to end: Double, count: Int) -> [Double?] {
        guard count > 0, end > start else { return [] }
        var sums = [Double](repeating: 0, count: count)
        var counts = [Int](repeating: 0, count: count)
        let column = columns[metric.rawValue]
        var index = firstIndex(atOrAfter: start)
        while index < times.count, times[index] < end {
            let value = times[index] == binStart ? Float.nan : column[index]
            if !value.isNaN {
                let slot = min(count - 1, Int((times[index] - start) / (end - start) * Double(count)))
                sums[slot] += Double(value)
                counts[slot] += 1
            }
            index += 1
        }
        if let binStart, binStart >= start, binStart < end, let value = currentValues[metric.rawValue], value.isFinite {
            let slot = min(count - 1, Int((binStart - start) / (end - start) * Double(count)))
            sums[slot] += value
            counts[slot] += 1
        }
        return (0..<count).map { counts[$0] > 0 ? sums[$0] / Double(counts[$0]) : nil }
    }

    public var currentValues: [Double?] {
        var values = sums.indices.map { counts[$0] > 0 ? sums[$0] / Double(counts[$0]) : nil }
        if motion.count >= 20 {
            let mean = motion.sum / Double(motion.count)
            values[Metric.activity.rawValue] = max(0, motion.squares / Double(motion.count) - mean * mean).squareRoot()
        }
        return values
    }

    public func flush() {
        guard let binStart else { return }
        flush(binStart)
    }

    #if DEBUG
    public func replace(with rows: [(time: Double, values: [Float])]) {
        times = rows.map(\.time)
        columns = columns.indices.map { column in rows.map { $0.values[column] } }
    }
    #endif

    private func firstIndex(atOrAfter time: Double) -> Int {
        var low = 0, high = times.count
        while low < high {
            let mid = (low + high) / 2
            if times[mid] < time { low = mid + 1 } else { high = mid }
        }
        return low
    }

    private func roll(to time: Double) {
        let start = (time / Self.binSeconds).rounded(.down) * Self.binSeconds
        guard let current = binStart else { binStart = start; return }
        guard start > current else { return }
        flush(current)
        sums = sums.map { _ in 0 }
        counts = counts.map { _ in 0 }
        motion = (0, 0, 0)
        binStart = start
    }

    private func flush(_ time: Double) {
        var values = sums.indices.map { counts[$0] > 0 ? Float(sums[$0] / Double(counts[$0])) : Float.nan }
        if motion.count >= 20 {
            let mean = motion.sum / Double(motion.count)
            values[Metric.activity.rawValue] = Float(max(0, motion.squares / Double(motion.count) - mean * mean).squareRoot())
        }
        guard values.contains(where: { !$0.isNaN }) else { return }
        append(time, values)
        write(time, values)
    }

    private func append(_ time: Double, _ values: [Float]) {
        if times.last == time {
            times.removeLast()
            for column in columns.indices { columns[column].removeLast() }
        }
        times.append(time)
        for (column, value) in values.enumerated() { columns[column].append(value) }
    }

    private func write(_ time: Double, _ values: [Float]) {
        guard persistent else { return }
        let day = Self.dayFormat.string(from: Date(timeIntervalSince1970: time))
        if file?.day != day {
            let cutoff = time - Self.keepDays * 86_400
            let cut = firstIndex(atOrAfter: cutoff)
            if cut > 0 {
                times.removeFirst(cut)
                for column in columns.indices { columns[column].removeFirst(cut) }
            }
            let oldest = Self.dayFormat.string(from: Date(timeIntervalSince1970: cutoff))
            for name in (try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? [] where name.hasSuffix(".bin") && String(name.prefix(10)) < oldest {
                try? FileManager.default.removeItem(at: directory.appendingPathComponent(name))
            }
            try? file?.handle.close()
            file = nil
            let url = directory.appendingPathComponent(day + ".bin")
            if !FileManager.default.fileExists(atPath: url.path) { FileManager.default.createFile(atPath: url.path, contents: nil) }
            guard let handle = try? FileHandle(forWritingTo: url), let end = try? handle.seekToEnd() else { failure = "History could not be saved."; return }
            // A crash can leave a torn record; cut it so later records stay aligned.
            let whole = end - end % UInt64(Self.recordBytes)
            if whole != end {
                do { try handle.truncate(atOffset: whole); try handle.seek(toOffset: whole) }
                catch { failure = "History could not be repaired."; try? handle.close(); return }
            }
            file = (day, handle)
        }
        var data = Data(capacity: Self.recordBytes)
        withUnsafeBytes(of: time.bitPattern.littleEndian) { data.append(contentsOf: $0) }
        for (index, value) in values.enumerated() {
            withUnsafeBytes(of: value.bitPattern.littleEndian) { data.append(contentsOf: $0) }
            withUnsafeBytes(of: UInt32(counts[index]).littleEndian) { data.append(contentsOf: $0) }
        }
        withUnsafeBytes(of: motion.sum.bitPattern.littleEndian) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: motion.squares.bitPattern.littleEndian) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: UInt32(motion.count).littleEndian) { data.append(contentsOf: $0) }
        do { try file?.handle.write(contentsOf: data) }
        catch { failure = "History could not be saved." }
    }

    private func load() {
        let cutoff = Self.dayFormat.string(from: Date().addingTimeInterval(-Self.keepDays * 86_400))
        let names = ((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []).filter { $0.hasSuffix(".bin") }.sorted()
        for name in names {
            let url = directory.appendingPathComponent(name)
            guard String(name.prefix(10)) >= cutoff else {
                try? FileManager.default.removeItem(at: url)
                continue
            }
            guard let data = try? Data(contentsOf: url) else { continue }
            data.withUnsafeBytes { raw in
                var offset = 0
                while offset + Self.recordBytes <= raw.count {
                    let time = Double(bitPattern: UInt64(littleEndian: raw.loadUnaligned(fromByteOffset: offset, as: UInt64.self)))
                    let values = columns.indices.map {
                        Float(bitPattern: UInt32(littleEndian: raw.loadUnaligned(fromByteOffset: offset + 8 + 8 * $0, as: UInt32.self)))
                    }
                    if time.isFinite, time >= (times.last ?? -.infinity) {
                        append(time, values)
                        binStart = time
                        counts = columns.indices.map { Int(UInt32(littleEndian: raw.loadUnaligned(fromByteOffset: offset + 12 + 8 * $0, as: UInt32.self))) }
                        sums = values.enumerated().map { $0.element.isFinite ? Double($0.element) * Double(counts[$0.offset]) : 0 }
                        let motionOffset = offset + 8 + 8 * Metric.allCases.count
                        motion = (Double(bitPattern: UInt64(littleEndian: raw.loadUnaligned(fromByteOffset: motionOffset, as: UInt64.self))),
                                  Double(bitPattern: UInt64(littleEndian: raw.loadUnaligned(fromByteOffset: motionOffset + 8, as: UInt64.self))),
                                  Int(UInt32(littleEndian: raw.loadUnaligned(fromByteOffset: motionOffset + 16, as: UInt32.self))))
                    }
                    offset += Self.recordBytes
                }
            }
        }
    }
}
