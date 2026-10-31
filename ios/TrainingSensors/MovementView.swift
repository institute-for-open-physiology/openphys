import SwiftUI

struct MovementView: View {
    let signals: LiveSignals
    @Environment(\.scenePhase) private var phase

    var body: some View {
        TimelineView(.animation(minimumInterval: 1.0 / 30, paused: phase != .active)) { tick in
            VStack(spacing: 10) {
                SectionTitle(title: NowContent.movement.title, info: NowContent.movement.info,
                             value: signals.accFresh ? (signals.moving ? "Tilt uncertain" : "Tilt") : "--")
                Canvas { context, size in
                    _ = tick.date
                    let g = signals.gravity
                    let pitch = atan2(g[2], max(1, hypot(g[0], g[1])))
                    let roll = atan2(g[1], -g[0])
                    let scale = min(1, size.width / 320)
                    func project(_ x: Double, _ y: Double, _ z: Double) -> CGPoint {
                        let a = x * cos(roll) - y * sin(roll), b = x * sin(roll) + y * cos(roll)
                        let c = b * cos(pitch) - z * sin(pitch), d = b * sin(pitch) + z * cos(pitch)
                        let depth = 420 / (420 + c * 0.4 + d * 0.5)
                        return CGPoint(x: size.width / 2 + a * depth * scale, y: size.height / 2 + (c * 0.48 - d * 0.85) * depth * scale)
                    }
                    for radius in [86.0, 117.0] {
                        var ring = Path()
                        for i in 0...64 {
                            let angle = Double(i) / 64 * .pi * 2
                            let point = CGPoint(x: size.width / 2 + cos(angle) * radius * scale, y: size.height / 2 + sin(angle) * radius * 0.56 * scale)
                            if i == 0 { ring.move(to: point) } else { ring.addLine(to: point) }
                        }
                        context.stroke(ring, with: .color(Palette.line), lineWidth: 0.6)
                    }
                    let outline = (0..<48).map { i -> (Double, Double) in
                        let angle = Double(i) / 48 * .pi * 2
                        return (cos(angle) * 72, sin(angle) * 36)
                    }
                    let top = outline.map { project($0.0, $0.1, 8) }
                    let bottom = outline.map { project($0.0, $0.1, -8) }
                    func polygon(_ points: [CGPoint]) -> Path {
                        Path { path in path.addLines(points); path.closeSubpath() }
                    }
                    var pod = context
                    pod.opacity = signals.accFresh ? (signals.moving ? 0.65 : 1) : 0.3
                    pod.fill(polygon(bottom), with: .color(Color(white: 0.13)))
                    for i in outline.indices {
                        let j = (i + 1) % outline.count
                        pod.fill(polygon([top[i], top[j], bottom[j], bottom[i]]), with: .color(Color(white: 0.23)))
                    }
                    pod.fill(polygon(top), with: .linearGradient(Gradient(colors: [Color(white: 0.28), Color(white: 0.12)]),
                                                               startPoint: CGPoint(x: size.width / 2 - 70, y: 50), endPoint: CGPoint(x: size.width / 2 + 70, y: 130)))
                    pod.stroke(polygon(top), with: .color(Color.white.opacity(0.1)), lineWidth: 0.6)
                    pod.draw(Text("POLAR").font(.system(size: 11, weight: .medium)).foregroundColor(Color(white: 0.65)), at: project(0, 0, 9))
                }.frame(height: Layout.movementHeight).accessibilityLabel("Estimated sensor tilt")
                HStack {
                    ForEach(0..<3) { index in
                        if index > 0 { Spacer() }
                        HStack(spacing: 8) {
                            Text(["X", "Y", "Z"][index]).foregroundStyle(Palette.muted)
                            Text(formatValue(signals.accFresh ? signals.acc.last(index) : nil)).monospacedDigit()
                        }
                    }
                    Text("mG").foregroundStyle(Palette.muted)
                }.font(.system(size: 12)).accessibilityElement(children: .combine)
            }
        }
    }
}
