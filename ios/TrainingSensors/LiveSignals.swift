import Foundation
import H10Kit
import QuartzCore

/// Recent sensor data on the CACurrentMediaTime clock, kept for drawing. Main thread only.
final class LiveSignals {
    /// Frames arrive in batches, so drawing runs this far behind to keep traces and the heart smooth.
    static let lag = 0.9
    static let ecgRate = 130.0
    static let accRate = 200.0

    final class Trace {
        private(set) var times: [Double] = []
        private(set) var values: [[Double]]
        let keep: Double

        init(channels: Int, keep: Double) {
            values = Array(repeating: [], count: channels)
            self.keep = keep
        }

        var lastTime: Double? { times.last }
        func last(_ channel: Int = 0) -> Double? { values[channel].last }

        func append(_ time: Double, _ sample: [Double]) {
            times.append(time)
            for channel in values.indices { values[channel].append(sample[channel]) }
            if let first = times.first, first < time - keep * 1.25 {
                let cut = index(after: time - keep)
                times.removeFirst(cut)
                for channel in values.indices { values[channel].removeFirst(cut) }
            }
        }

        /// First index whose time is later than the given time.
        func index(after time: Double) -> Int {
            var low = 0, high = times.count
            while low < high {
                let mid = (low + high) / 2
                if times[mid] <= time { low = mid + 1 } else { high = mid }
            }
            return low
        }
    }

    let history = History(directory: FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0].appendingPathComponent("history"))
    let ecg = Trace(channels: 1, keep: 30)
    let acc = Trace(channels: 3, keep: 30)
    let heartRate = Trace(channels: 1, keep: 4 * 3600)
    let rr = Trace(channels: 1, keep: 3600)
    let rssi = Trace(channels: 1, keep: 900)
    /// Packet delay in ms and its rolling mean over the last 1000 PMD frames.
    let delay = Trace(channels: 2, keep: 900)
    private(set) var beats: [Double] = []
    private(set) var ecgScale = ECGDisplayScale()
    private(set) var gravity = [0.0, 0.0, 0.0]
    private(set) var moving = false

    private var lastECG = -Double.infinity
    private var lastACC = -Double.infinity
    private var detector = BeatDetector(rate: LiveSignals.ecgRate)
    private var ecgIndex = 0
    private var ecgTimes: [Double] = []
    private var ecgNext: Double?
    private var accNext: Double?
    private var delays: [Double] = []
    private var delaySum = 0.0

    var ecgFresh: Bool { DisplayClock.uptime - lastECG < 2.5 }
    var accFresh: Bool { DisplayClock.uptime - lastACC < 2.5 }
    var heartRateFresh: Bool { heartRate.lastTime.map { DisplayClock.uptime - $0 < 2.5 } ?? false }

    func addPMD(_ bytes: [UInt8], unixNs: Int64, offsetMinutes: Int?) {
        guard let frame = PMD.frame(bytes) else { return }
        let now = DisplayClock.uptime
        if let offsetMinutes {
            let receivedNs = unixNs - PMD.epoch2000UnixNs + Int64(offsetMinutes) * 60_000_000_000
            addDelay((Double(receivedNs) - Double(frame.timestampNs)) / 1_000_000, now)
        }
        if let samples = PMD.ecgMicrovolts(frame), !samples.isEmpty {
            addECG(samples, now)
        } else if let samples = PMD.accMilliG(frame), !samples.isEmpty {
            addACC(samples, now)
        }
    }

    func addECG(_ samples: [Int32], _ arrival: Double) {
        let (start, continued) = place(samples.count, rate: Self.ecgRate, arrival: arrival, next: &ecgNext)
        if !continued {
            detector = BeatDetector(rate: Self.ecgRate)
            ecgTimes.removeAll()
        }
        for (i, sample) in samples.enumerated() {
            let time = start + Double(i) / Self.ecgRate
            ecg.append(time, [Double(sample)])
            ecgTimes.append(time)
            if let peak = detector.push(Float(sample), index: ecgIndex), ecgIndex - peak < ecgTimes.count {
                beats.append(ecgTimes[ecgTimes.count - 1 - (ecgIndex - peak)])
            }
            ecgIndex += 1
        }
        if ecgTimes.count > 260 { ecgTimes.removeFirst(ecgTimes.count - 130) }
        if beats.count > 60 { beats.removeFirst(beats.count - 60) }
        lastECG = arrival
        let visible = ecg.values[0][ecg.index(after: arrival - 5)...]
        ecgScale.update(minimum: visible.min() ?? -500, maximum: visible.max() ?? 1500, at: arrival)
    }

    func addACC(_ samples: [[Int32]], _ arrival: Double) {
        let (start, continued) = place(samples.count, rate: Self.accRate, arrival: arrival, next: &accNext)
        if !continued, let first = samples.first { gravity = first.map(Double.init) }
        let wallOffset = Date().timeIntervalSince1970 - arrival
        for (i, sample) in samples.enumerated() {
            let time = start + Double(i) / Self.accRate
            acc.append(time, sample.map(Double.init))
            for channel in 0..<3 { gravity[channel] += 0.005 * (Double(sample[channel]) - gravity[channel]) }
            let magnitude = sample.reduce(0.0) { $0 + Double($1) * Double($1) }.squareRoot()
            history.addMotion(magnitude, at: wallOffset + time)
        }
        lastACC = arrival
        if let sample = samples.last {
            let residual = zip(sample, gravity).reduce(0.0) { $0 + pow(Double($1.0) - $1.1, 2) }.squareRoot()
            let magnitude = gravity.reduce(0.0) { $0 + $1 * $1 }.squareRoot()
            moving = residual > 120 || abs(magnitude - 1000) > 150
        }
    }

    func addDelay(_ ms: Double, _ arrival: Double) {
        delays.append(ms)
        delaySum += ms
        if delays.count > 1000 { delaySum -= delays.removeFirst() }
        delay.append(arrival, [ms, delaySum / Double(delays.count)])
        history.add(.delay, ms)
    }

    func addHeartRate(_ bpm: Int, rr intervals: [Double]) {
        let now = DisplayClock.uptime
        heartRate.append(now, [Double(bpm)])
        history.add(.heartRate, Double(bpm))
        for interval in intervals { history.add(.rr, interval) }
        for (i, value) in intervals.enumerated() { rr.append(now - Double(intervals.count - 1 - i) * 0.001, [value]) }
    }

    func addRSSI(_ dbm: Int) {
        rssi.append(DisplayClock.uptime, [Double(dbm)])
        history.add(.rssi, Double(dbm))
    }

    func average(_ trace: Trace, over window: Double) -> Double? {
        let values = trace.values[0][trace.index(after: DisplayClock.uptime - window)...]
        return values.count >= 2 ? values.reduce(0, +) / Double(values.count) : nil
    }

    /// Heart contraction from 0 to 1 at a display time: a strong beat, then a softer echo.
    func pulse(at time: Double, bpm: Int?) -> Double {
        let since: Double
        if ecgFresh {
            guard let beat = beats.last(where: { $0 <= time }) else { return 0 }
            since = time - beat
        } else if heartRateFresh, let bpm, bpm > 0 {
            since = time.truncatingRemainder(dividingBy: 60 / Double(bpm))
        } else {
            return 0
        }
        func bump(_ center: Double, _ width: Double) -> Double { exp(-pow((since - center) / width, 2)) }
        return min(1, bump(0.06, 0.05) + 0.4 * bump(0.32, 0.07))
    }

    /// Spreads a frame's samples evenly and keeps consecutive frames continuous despite arrival jitter.
    private func place(_ count: Int, rate: Double, arrival: Double, next: inout Double?) -> (Double, Bool) {
        let ideal = arrival - Double(count - 1) / rate
        let continued = next.map { abs($0 - ideal) < 0.5 } ?? false
        let start = continued ? next! + (ideal - next!) * 0.02 : ideal
        next = start + Double(count) / rate
        return (start, continued)
    }
}
