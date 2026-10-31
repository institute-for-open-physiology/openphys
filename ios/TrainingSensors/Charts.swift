import QuartzCore
import SwiftUI
import H10Kit

struct PlotPoint {
    let time: Double
    let value: Double?
}

struct PlotBand {
    let low: Double
    let high: Double
    let color: Color
}

struct SignalPlot: View {
    let points: [PlotPoint]
    let start: Double
    let end: Double
    let color: Color
    var range: ClosedRange<Double>? = nil
    var style = Style.line
    var bands: [PlotBand] = []
    var marks: [SensorMark] = []
    var onSelectMark: ((SensorMark) -> Void)? = nil
    var showAverage = true
    var showExtremes = false
    var selectionTolerance: Double? = nil
    @Binding var selection: Double?
    enum Style { case line, dots, bars }

    private var values: [Double] { points.compactMap(\.value).filter(\.isFinite) }
    private var bounds: ClosedRange<Double> {
        if let range { return range }
        let low = values.min() ?? 0, high = values.max() ?? 100
        let padding = max(5, (high - low) * 0.15)
        return (style == .bars ? 0 : low - padding)...(high + padding)
    }
    var body: some View {
        GeometryReader { geometry in
            Canvas { context, size in
                let left = 34.0, right = size.width - 4, top = onSelectMark == nil ? 12.0 : 42.0, bottom = size.height - 24
                let scale = bounds
                func x(_ time: Double) -> Double { left + (time - start) / max(1, end - start) * (right - left) }
                func y(_ value: Double) -> Double { bottom - (value - scale.lowerBound) / (scale.upperBound - scale.lowerBound) * (bottom - top) }
                for band in bands {
                    let a = y(min(band.high, scale.upperBound)), b = y(max(band.low, scale.lowerBound))
                    if b > a { context.fill(Path(CGRect(x: left, y: a, width: right - left, height: b - a)), with: .color(band.color.opacity(0.065))) }
                }
                for fraction in [0.0, 0.5, 1.0] {
                    let value = scale.lowerBound + fraction * (scale.upperBound - scale.lowerBound)
                    context.stroke(Path { $0.move(to: CGPoint(x: left, y: y(value))); $0.addLine(to: CGPoint(x: right, y: y(value))) }, with: .color(Palette.line), lineWidth: 0.5)
                    context.draw(Text(formatValue(value)).font(.system(size: 10)).foregroundColor(Palette.muted), at: CGPoint(x: left - 8, y: y(value)), anchor: .trailing)
                    let stamp = start + fraction * (end - start)
                    let date = Date(timeIntervalSince1970: stamp)
                    let label = end - start >= 86_400 ? date.formatted(.dateTime.weekday(.abbreviated).hour().minute()) : date.formatted(.dateTime.hour().minute())
                    context.draw(Text(label).font(.system(size: 10)).foregroundColor(Palette.muted), at: CGPoint(x: x(stamp), y: size.height - 2), anchor: fraction == 0 ? .bottomLeading : fraction == 1 ? .bottomTrailing : .bottom)
                }
                if showAverage, !values.isEmpty {
                    let average = values.reduce(0, +) / Double(values.count)
                    context.stroke(Path { $0.move(to: CGPoint(x: left, y: y(average))); $0.addLine(to: CGPoint(x: right, y: y(average))) }, with: .color(Palette.faint), style: StrokeStyle(lineWidth: 0.7, dash: [3, 4]))
                    if showExtremes {
                        context.draw(Text("Avg " + formatValue(average)).font(.system(size: 11)).foregroundColor(Palette.muted), at: CGPoint(x: right, y: y(average) - 7), anchor: .bottomTrailing)
                    }
                }
                var path = Path(), drawing = false
                for point in points {
                    guard point.time >= start, point.time <= end, let value = point.value, value.isFinite else { drawing = false; continue }
                    let position = CGPoint(x: x(point.time), y: y(value))
                    switch style {
                    case .bars:
                        let width = max(1, (right - left) / Double(max(1, points.count)) * 0.7)
                        context.fill(Path(roundedRect: CGRect(x: position.x - width / 2, y: position.y, width: width, height: max(0, bottom - position.y)), cornerRadius: 1.5), with: .color(color.opacity(0.75)))
                    case .dots:
                        context.fill(Path(ellipseIn: CGRect(x: position.x - 1.7, y: position.y - 1.7, width: 3.4, height: 3.4)), with: .color(color.opacity(0.85)))
                    case .line:
                        if drawing { path.addLine(to: position) } else { path.move(to: position); drawing = true }
                    }
                }
                context.stroke(path, with: .color(color), style: StrokeStyle(lineWidth: 1.8, lineCap: .round, lineJoin: .round))
                if showExtremes, selection == nil {
                    let visible = points.filter { (start...end).contains($0.time) && $0.value?.isFinite == true }
                    for high in [false, true] {
                        let point = high ? visible.max(by: { $0.value! < $1.value! }) : visible.min(by: { $0.value! < $1.value! })
                        if let point, let value = point.value {
                            let position = CGPoint(x: x(point.time), y: y(value))
                            context.fill(Path(ellipseIn: CGRect(x: position.x - 2, y: position.y - 2, width: 4, height: 4)), with: .color(color))
                            context.draw(Text(formatValue(value) + (high ? " high" : " low")).font(.system(size: 11)).foregroundColor(Palette.muted),
                                         at: CGPoint(x: min(right - 26, max(left + 26, position.x)), y: position.y + (high ? -13 : 13)))
                        }
                    }
                }
                for mark in marks where mark.time.timeIntervalSince1970 >= start && mark.time.timeIntervalSince1970 <= end {
                    let position = x(mark.time.timeIntervalSince1970)
                    context.stroke(Path { $0.move(to: CGPoint(x: position, y: top)); $0.addLine(to: CGPoint(x: position, y: bottom)) },
                                   with: .color(Palette.faint), style: StrokeStyle(lineWidth: 0.7, dash: [2, 3]))
                }
                if let selection, selection >= start, selection <= end {
                    let position = x(selection)
                    context.stroke(Path { $0.move(to: CGPoint(x: position, y: top)); $0.addLine(to: CGPoint(x: position, y: bottom)) }, with: .color(Palette.muted), style: StrokeStyle(lineWidth: 1, dash: [2, 3]))
                    if let point = points.min(by: { abs($0.time - selection) < abs($1.time - selection) }),
                       abs(point.time - selection) <= (selectionTolerance ?? (end - start) / Double(max(1, points.count))), let value = point.value {
                        context.fill(Path(ellipseIn: CGRect(x: x(point.time) - 4, y: y(value) - 4, width: 8, height: 8)), with: .color(color))
                    }
                }
                if values.isEmpty {
                    context.draw(Text("No data in this range").font(.system(size: 13)).foregroundColor(Palette.muted), at: CGPoint(x: (left + right) / 2, y: (top + bottom) / 2))
                }
            }
            .contentShape(Rectangle())
            // Let vertical scrolling begin before chart inspection.
            .simultaneousGesture(DragGesture(minimumDistance: 20).onChanged { gesture in
                guard abs(gesture.translation.width) > abs(gesture.translation.height) else { return }
                let fraction = max(0, min(1, (gesture.location.x - 34) / max(1, geometry.size.width - 38)))
                selection = start + fraction * (end - start)
            }.onEnded { _ in selection = nil })
            .accessibilityElement(children: .ignore)
            .accessibilityLabel("Chart")
            .accessibilityValue(values.isEmpty ? "No data" : "Low \(formatValue(values.min())), average \(formatValue(values.reduce(0, +) / Double(values.count))), high \(formatValue(values.max()))")
            .accessibilityAdjustableAction { direction in
                let step = (end - start) / Double(max(1, points.count))
                selection = max(start, min(end, (selection ?? end) + (direction == .increment ? step : -step)))
            }
            if let onSelectMark {
                ForEach(Array(groupMarks(width: geometry.size.width).enumerated()), id: \.offset) { _, group in
                    let first = group[0]
                    let time = group.reduce(0.0) { $0 + $1.time.timeIntervalSince1970 } / Double(group.count)
                    let x = 34 + (time - start) / max(1, end - start) * (geometry.size.width - 38)
                    Group {
                        if group.count == 1 {
                            Button { onSelectMark(first) } label: { markLabel(first, count: 1) }
                                .accessibilityIdentifier("chart-mark-" + first.id)
                        } else {
                            Menu {
                                ForEach(group) { mark in
                                    Button(mark.displayLabel + " · " + mark.time.formatted(date: .omitted, time: .standard)) { onSelectMark(mark) }
                                }
                            } label: { markLabel(first, count: group.count) }
                            .accessibilityIdentifier("chart-marks-" + first.id)
                        }
                    }
                    .buttonStyle(.plain)
                    .position(x: min(geometry.size.width - 52, max(52, x)), y: 18)
                }
            }
        }
    }

    private func markLabel(_ mark: SensorMark, count: Int) -> some View {
        VStack(spacing: 3) {
            HStack(spacing: 4) {
                Circle().fill(Palette.ink).frame(width: 4, height: 4)
                Text(mark.displayLabel).lineLimit(1)
                if count > 1 { Text("+\(count - 1)") }
            }.font(.system(size: 11, weight: .medium))
            Text(mark.time.formatted(date: .omitted, time: .standard))
                .font(.system(size: 9)).monospacedDigit().foregroundStyle(Palette.muted)
        }.frame(width: 100, height: 36).contentShape(Rectangle())
    }

    private func groupMarks(width: Double) -> [[SensorMark]] {
        let visible = marks.filter { (start...end).contains($0.time.timeIntervalSince1970) }.sorted { $0.time < $1.time }
        func position(_ mark: SensorMark) -> Double {
            let x = 34 + (mark.time.timeIntervalSince1970 - start) / max(1, end - start) * (width - 38)
            return min(width - 52, max(52, x))
        }
        var groups: [[SensorMark]] = []
        for mark in visible {
            if let previous = groups.last?.last,
               position(mark) - position(previous) < 104 {
                groups[groups.count - 1].append(mark)
            } else { groups.append([mark]) }
        }
        return groups
    }
}

struct ECGPlot: View {
    let signals: LiveSignals
    @State private var inspectedEnd: Double?
    @State private var inspectedTime: Double?
    @Environment(\.scenePhase) private var phase
    var body: some View {
        TimelineView(.animation(minimumInterval: 1.0 / 60, paused: phase != .active)) { tick in
            let stamp = tick.date
            Canvas { context, size in
                _ = stamp
                let end = inspectedEnd ?? (DisplayClock.uptime - LiveSignals.lag), span = 4.0
                let start = end - span, trace = signals.ecg
                let first = trace.index(after: start), last = trace.index(after: end)
                let left = 30.0, top = 28.0, bottom = size.height - 24
                let low = signals.ecgScale.lower, high = signals.ecgScale.upper
                func x(_ time: Double) -> Double { left + (time - start) / span * (size.width - left) }
                func y(_ value: Double) -> Double { bottom - (value - low) / (high - low) * (bottom - top) }
                for index in 0...20 {
                    let position = left + Double(index) / 20 * (size.width - left)
                    context.stroke(Path { $0.move(to: CGPoint(x: position, y: top)); $0.addLine(to: CGPoint(x: position, y: bottom)) }, with: .color(Palette.line.opacity(index % 5 == 0 ? 0.7 : 0.25)), lineWidth: 0.5)
                }
                for value in stride(from: 0.0, through: high, by: 250) {
                    context.stroke(Path { $0.move(to: CGPoint(x: left, y: y(value))); $0.addLine(to: CGPoint(x: size.width, y: y(value))) }, with: .color(Palette.line.opacity(0.5)), lineWidth: 0.5)
                }
                context.draw(Text("1 mV").font(.system(size: 9)).foregroundColor(Palette.muted), at: CGPoint(x: 0, y: y(1000)), anchor: .leading)
                context.draw(Text("0").font(.system(size: 9)).foregroundColor(Palette.muted), at: CGPoint(x: 17, y: y(0)), anchor: .leading)
                var path = Path()
                for index in first..<last {
                    let point = CGPoint(x: x(trace.times[index]), y: y(trace.values[0][index]))
                    if index == first || trace.times[index] - trace.times[index - 1] > 0.1 { path.move(to: point) } else { path.addLine(to: point) }
                }
                context.stroke(path, with: .color(Palette.ink), style: StrokeStyle(lineWidth: 1.35, lineCap: .round, lineJoin: .round))
                if let inspectedTime, let index = signals.beats.indices.dropFirst().min(by: { abs(signals.beats[$0] - inspectedTime) < abs(signals.beats[$1] - inspectedTime) }),
                   abs(signals.beats[index] - inspectedTime) < 1.5 {
                    let interval = signals.beats[index] - signals.beats[index - 1]
                    if (0.25...2.5).contains(interval) {
                        context.draw(Text("RR \(Int(interval * 1000)) ms").font(.system(size: 12)).foregroundColor(Palette.muted), at: CGPoint(x: size.width, y: 7), anchor: .trailing)
                        context.stroke(Path { $0.move(to: CGPoint(x: x(inspectedTime), y: top)); $0.addLine(to: CGPoint(x: x(inspectedTime), y: bottom)) }, with: .color(Palette.faint), lineWidth: 0.5)
                    }
                }
                let length = (size.width - left) / span
                context.stroke(Path { $0.move(to: CGPoint(x: size.width - length, y: size.height - 15)); $0.addLine(to: CGPoint(x: size.width, y: size.height - 15)) }, with: .color(Palette.faint), lineWidth: 1)
                context.draw(Text("1 s").font(.system(size: 9)).foregroundColor(Palette.muted), at: CGPoint(x: size.width - length / 2, y: size.height - 5))
                if first == last { context.draw(Text("Waiting for ECG").font(.system(size: 13)).foregroundColor(Palette.muted), at: CGPoint(x: size.width / 2, y: size.height / 2)) }
            }
            .overlay {
                GeometryReader { geometry in
                    Color.clear.contentShape(Rectangle()).simultaneousGesture(DragGesture(minimumDistance: 20).onChanged { gesture in
                        guard abs(gesture.translation.width) > abs(gesture.translation.height) else { return }
                        if inspectedEnd == nil { inspectedEnd = DisplayClock.uptime - LiveSignals.lag }
                        let fraction = min(1, max(0, (gesture.location.x - 30) / max(1, geometry.size.width - 30)))
                        inspectedTime = (inspectedEnd ?? DisplayClock.uptime) - 4 + fraction * 4
                    }.onEnded { _ in inspectedEnd = nil; inspectedTime = nil })
                }
            }
        }
        .accessibilityLabel("Live ECG, four seconds")
    }
}

struct BeatingHeart: View {
    let signals: LiveSignals
    let bpm: Int?
    let live: Bool
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.scenePhase) private var phase
    var body: some View {
        TimelineView(.animation(minimumInterval: 1.0 / 30, paused: phase != .active || reduceMotion || !live)) { _ in
            let pulse = live && !reduceMotion ? signals.pulse(at: DisplayClock.uptime - LiveSignals.lag, bpm: bpm) : 0
            ZStack {
                RadialGradient(colors: [Palette.heart.opacity(live ? 0.14 + pulse * 0.1 : 0.02), .clear], center: .center, startRadius: 0, endRadius: 120)
                    .frame(width: 280, height: 240)
                Image(systemName: "heart.fill").font(.system(size: 82, weight: .regular))
                    .foregroundStyle(live ? Palette.heart : Palette.faint)
                    .scaleEffect(1 + 0.1 * pulse)
            }
        }
        .accessibilityHidden(true)
    }
}
