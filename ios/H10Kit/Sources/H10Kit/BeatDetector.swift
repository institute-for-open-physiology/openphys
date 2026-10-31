import Foundation

/// Causal R-peak detector for the live heart: slope energy over 150 ms against an adaptive threshold.
public struct BeatDetector {
    private let window: Int
    private let refractory: Int
    private let warmup: Int
    private let longest: Int
    private let decay: Double
    private var previous: [Float] = []
    private var energy: [Double]
    private var slot = 0
    private var sum = 0.0
    private var level = 0.0
    private var baseline: Float?
    private var recent: [(index: Int, height: Float)] = []
    private var beatStart: Int?
    private var best: (index: Int, height: Float) = (0, 0)
    private var lastBeat = Int.min / 2
    private var seen = 0

    public init(rate: Double) {
        window = max(1, Int(rate * 0.15))
        refractory = Int(rate * 0.25)
        warmup = Int(rate * 2)
        longest = Int(rate * 0.3)
        decay = pow(0.5, 1 / (rate * 3))
        energy = Array(repeating: 0, count: window)
    }

    /// Feeds one sample and returns the R-peak sample index once that beat's QRS has passed.
    public mutating func push(_ value: Float, index: Int) -> Int? {
        seen += 1
        let base = baseline.map { $0 + (value - $0) * 0.02 } ?? value
        baseline = base
        let height = abs(value - base)
        recent.append((index, height))
        if recent.count > window { recent.removeFirst() }
        let slope = previous.count == 2 ? Double(value - previous[0]) : 0
        previous.append(value)
        if previous.count > 2 { previous.removeFirst() }
        sum += slope * slope - energy[slot]
        energy[slot] = slope * slope
        slot = (slot + 1) % window
        let integrated = max(0, sum) / Double(window)
        level = max(integrated, level * decay)
        let above = seen > warmup && integrated > level * 0.3

        guard let start = beatStart else {
            if above, index - lastBeat > refractory, let peak = recent.max(by: { $0.height < $1.height }) {
                beatStart = index
                best = peak
            }
            return nil
        }
        if height > best.height { best = (index, height) }
        if above && index - start < longest { return nil }
        beatStart = nil
        lastBeat = best.index
        return best.index
    }
}
