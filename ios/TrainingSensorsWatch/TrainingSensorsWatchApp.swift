import H10Kit
import SwiftUI

@main
struct TrainingSensorsWatchApp: App {
    @StateObject private var model = WatchModel()
    @Environment(\.scenePhase) private var phase
    var body: some Scene {
        WindowGroup {
            WatchView(model: model)
                .task(id: phase) {
                    guard phase == .active else { return }
                    while !Task.isCancelled {
                        model.refresh()
                        try? await Task.sleep(for: .seconds(1))
                    }
                }
        }
    }
}

private struct WatchView: View {
    @ObservedObject var model: WatchModel
    @Environment(\.scenePhase) private var phase
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    private let heart = Color(red: 1, green: 0.38, blue: 0.43)

    var body: some View {
        TimelineView(.animation(minimumInterval: reduceMotion ? 1 : 1.0 / 30, paused: phase != .active)) { tick in
            let now = tick.date
            let current = phase == .active && model.snapshot?.isCurrent(at: now) == true
            let bpm = current ? model.snapshot?.currentHeartRate(at: now) : nil
            let connected = current && model.snapshot?.connected == true
            let ecg = current ? model.snapshot?.currentECG(at: now) : nil
            GeometryReader { geometry in
                VStack(spacing: 8) {
                    HStack(spacing: 5) {
                        Circle().fill(connected ? Color.green : Color.gray).frame(width: 4, height: 4)
                        Text(phase != .active ? "Raise wrist" : current ? (connected ? "H10" : "Disconnected") : "Waiting for iPhone")
                            .lineLimit(1)
                        Spacer(minLength: 0)
                    }.font(.system(size: 11)).foregroundStyle(.secondary)
                    HStack(alignment: .firstTextBaseline, spacing: 4) {
                        Image(systemName: "heart.fill").font(.system(size: 17)).foregroundStyle(bpm == nil ? .gray : heart)
                            .accessibilityHidden(true).padding(.trailing, 3)
                        Text(bpm.map(String.init) ?? "--")
                            .font(.system(size: 58, weight: .semibold, design: .rounded)).monospacedDigit().tracking(-2)
                        Text("bpm").font(.system(size: 12)).foregroundStyle(.secondary)
                    }.minimumScaleFactor(0.8).lineLimit(1).frame(maxWidth: .infinity, maxHeight: .infinity)
                    VStack(spacing: 5) {
                        HStack {
                            Text("ECG")
                            Spacer()
                            Text("3 s")
                        }.font(.system(size: 10)).foregroundStyle(.secondary)
                        Canvas { context, size in
                            guard let ecg, let snapshot = model.snapshot else {
                                context.draw(Text(phase != .active ? "" : connected ? "Waiting for ECG" : "").font(.system(size: 11)).foregroundColor(.gray), at: CGPoint(x: size.width / 2, y: size.height / 2))
                                return
                            }
                            let end = now.timeIntervalSince(snapshot.sentAt) - 1.5, start = end - 3
                            for index in 0...12 {
                                let x = size.width * Double(index) / 12
                                context.stroke(Path { $0.move(to: CGPoint(x: x, y: 0)); $0.addLine(to: CGPoint(x: x, y: size.height)) }, with: .color(.white.opacity(0.07)), lineWidth: 0.5)
                            }
                            for index in 1...3 {
                                let y = size.height * Double(index) / 4
                                context.stroke(Path { $0.move(to: CGPoint(x: 0, y: y)); $0.addLine(to: CGPoint(x: size.width, y: y)) }, with: .color(.white.opacity(0.07)), lineWidth: 0.5)
                            }
                            var path = Path(), previous: Double?
                            for index in ecg.offsets.indices {
                                let time = ecg.offsets[index]
                                guard time >= start, time <= end else { continue }
                                let point = CGPoint(x: (time - start) / 3 * size.width,
                                                    y: size.height * (1 - (Double(ecg.microvolts[index]) - ecg.lower) / (ecg.upper - ecg.lower)))
                                if let previous, time - previous <= 0.1 { path.addLine(to: point) } else { path.move(to: point) }
                                previous = time
                            }
                            context.stroke(path, with: .color(heart), style: StrokeStyle(lineWidth: 1.2, lineCap: .round, lineJoin: .round))
                        }.clipped().frame(height: min(82, max(48, geometry.size.height * 0.32)))
                            .accessibilityLabel(ecg == nil ? "ECG unavailable" : "H10 ECG, three seconds")
                    }
                    if let problem = model.problem { Text(problem).foregroundStyle(.red).font(.system(size: 10)) }
                }.padding(.horizontal, 6).frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }.containerBackground(.black, for: .navigation)
    }
}
