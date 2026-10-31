import H10Kit
import QuartzCore
import SwiftUI

struct HistoryView: View {
    @ObservedObject var collector: H10Collector
    @State private var zoom = log(600.0)
    @State private var pastEnd: Double?
    @State private var selection: Double?
    @State private var pinchStart: Double?
    private var span: Double { exp(zoom) }
    private let anchors = [600.0, 3600, 21600, 86400, 604800]

    var body: some View {
        Page {
            PageTitle(title: "History")
            TimelineView(.periodic(from: .now, by: 1)) { context in
                let end = pastEnd ?? DisplayClock.date(context.date).timeIntervalSince1970
                let start = end - span
                VStack(alignment: .leading, spacing: Layout.sectionGap) {
                    controls(start: start, end: end, now: DisplayClock.date(context.date).timeIntervalSince1970)
                    ForEach(History.Metric.allCases, id: \.rawValue) { metric in
                        HistoryMetricView(collector: collector, metric: metric, start: start, end: end, live: pastEnd == nil, selection: $selection)
                    }
                    if let failure = collector.signals.history.failure { Text(failure).font(.footnote).foregroundStyle(Palette.bad) }
                }
                .simultaneousGesture(MagnifyGesture().onChanged { value in
                    if pinchStart == nil { pinchStart = zoom }
                    zoom = max(log(300.0), min(log(604800.0), (pinchStart ?? zoom) - log(Double(value.magnification))))
                    selection = nil
                }.onEnded { _ in pinchStart = nil })
            }
        }
    }
    private func controls(start: Double, end: Double, now: Double) -> some View {
        VStack(spacing: 14) {
            HStack {
                Button { pastEnd = end - span; selection = nil } label: { Image(systemName: "chevron.left").frame(width: 44, height: 44) }
                    .disabled(start <= (collector.signals.history.earliest ?? start)).accessibilityLabel("Earlier")
                Spacer()
                VStack(spacing: 4) {
                    Text(formatSpan(span)).font(.system(size: 17, weight: .semibold))
                    Text(formatRange(start, end)).font(.system(size: 11)).foregroundStyle(Palette.muted)
                }
                Spacer()
                Button { pastEnd = end + span >= now ? nil : end + span; selection = nil } label: { Image(systemName: "chevron.right").frame(width: 44, height: 44) }
                    .disabled(pastEnd == nil).accessibilityLabel("Later")
            }.buttonStyle(.plain)
            VStack(spacing: 0) {
                Slider(value: $zoom, in: log(300)...log(604800), onEditingChanged: { editing in
                    if !editing, let anchor = anchors.min(by: { abs(log($0) - zoom) < abs(log($1) - zoom) }), abs(log(anchor) - zoom) < 0.16 { zoom = log(anchor) }
                    selection = nil
                }).tint(Palette.ink).accessibilityLabel("Time range").accessibilityValue(formatSpan(span))
                GeometryReader { geometry in
                    ForEach([300.0, 3600, 86400, 604800], id: \.self) { value in
                        let position = (log(value) - log(300)) / (log(604800) - log(300))
                        Text(value == 300 ? "5m" : value == 3600 ? "1h" : value == 86400 ? "1d" : "1w")
                            .font(.system(size: 10)).foregroundStyle(Palette.muted)
                            .position(x: 12 + position * (geometry.size.width - 24), y: 7)
                    }
                }.frame(height: 14)
            }
            if pastEnd != nil { Button("Back to now") { pastEnd = nil; selection = nil }.font(.system(size: 12)) }
        }
    }

    private func formatSpan(_ seconds: Double) -> String {
        if seconds >= 518_400 { return "\(Int((seconds / 86_400).rounded())) days" }
        if seconds >= 86_400 { return String(format: "%.1f days", seconds / 86_400) }
        if seconds >= 3600 { return String(format: "%.1f hours", seconds / 3600) }
        return "\(Int((seconds / 60).rounded())) minutes"
    }
    private func formatRange(_ start: Double, _ end: Double) -> String {
        let a = Date(timeIntervalSince1970: start), b = Date(timeIntervalSince1970: end)
        if span >= 86_400 { return a.formatted(.dateTime.month(.abbreviated).day()) + " to " + b.formatted(.dateTime.month(.abbreviated).day()) }
        return a.formatted(.dateTime.hour().minute()) + " to " + b.formatted(.dateTime.hour().minute())
    }
}

private struct HistoryMetricView: View {
    @ObservedObject var collector: H10Collector
    let metric: History.Metric
    let start: Double
    let end: Double
    let live: Bool
    @Binding var selection: Double?

    var body: some View {
        let count = min(240, max(2, Int((end - start) / History.binSeconds)))
        let series = collector.signals.history.series(metric, from: start, to: end, count: count)
        let points = series.enumerated().map { PlotPoint(time: start + (Double($0.offset) + 0.5) / Double(count) * (end - start), value: $0.element) }
        let value = selection.map { stamp -> Double? in
            let index = min(count - 1, max(0, Int((stamp - start) / (end - start) * Double(count))))
            return series[index]
        } ?? (live ? latest : series.last ?? nil)
        let values = series.compactMap { $0 }
        VStack(alignment: .leading, spacing: 8) {
            SectionTitle(title: title, info: explanation, value: formatValue(value, unit: unit))
            SignalPlot(points: points, start: start, end: end, color: color, range: range, style: style, bands: bands,
                       showAverage: metric != .activity, selection: $selection).frame(height: Layout.chartHeight)
            HStack {
                if metric != .activity {
                    Text("Average " + formatValue(values.isEmpty ? nil : values.reduce(0, +) / Double(values.count), unit: unit))
                } else { Text("Variation in acceleration") }
                Spacer()
                if let selection { Text(Date(timeIntervalSince1970: selection).formatted(date: .omitted, time: .shortened)).monospacedDigit() }
                else if live { Text("Latest") }
            }.font(.system(size: 11)).foregroundStyle(Palette.muted)
        }
    }

    private var latest: Double? {
        guard collector.state == "Streaming" else { return nil }
        let signals = collector.signals
        switch metric {
        case .heartRate: return signals.heartRateFresh ? collector.heartRate.map(Double.init) : nil
        case .rr: return signals.rr.lastTime.map { DisplayClock.uptime - $0 < 3 } == true ? collector.rrMs : nil
        case .rssi: return collector.rssi.map(Double.init)
        case .delay: return signals.delay.lastTime.map { DisplayClock.uptime - $0 < 3 } == true ? signals.delay.last() : nil
        case .activity: return signals.accFresh ? signals.history.currentValues[metric.rawValue] : nil
        }
    }
    private var content: Section {
        switch metric {
        case .heartRate: HistoryContent.heartRate
        case .rr: HistoryContent.rr
        case .activity: HistoryContent.activity
        case .rssi: HistoryContent.rssi
        case .delay: HistoryContent.delay
        }
    }
    private var title: String { content.title }
    private var unit: String { content.unit }
    private var color: Color {
        switch metric { case .heartRate, .rr: Palette.heart; case .activity: Palette.motion; case .rssi: Level.signal(collector.rssi).color; case .delay: Palette.warn }
    }
    private var style: SignalPlot.Style { metric == .rr ? .dots : metric == .activity ? .bars : .line }
    private var range: ClosedRange<Double>? { metric == .rssi ? -100 ... -30 : nil }
    private var bands: [PlotBand] {
        switch metric {
        case .rssi: [PlotBand(low: -100, high: -80, color: Palette.bad), PlotBand(low: -80, high: -65, color: Palette.warn), PlotBand(low: -65, high: -30, color: Palette.up)]
        case .delay: [PlotBand(low: -10000, high: 100, color: Palette.up), PlotBand(low: 100, high: 250, color: Palette.warn), PlotBand(low: 250, high: 10000, color: Palette.bad)]
        default: []
        }
    }
    private var explanation: String { content.info ?? "" }
}
