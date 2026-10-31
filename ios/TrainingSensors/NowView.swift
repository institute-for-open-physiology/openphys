import QuartzCore
import SwiftUI
import UIKit
import H10Kit

struct NowView: View {
    @ObservedObject var collector: H10Collector
    var openDevice: () -> Void = {}
    @State private var selection: Double?
    @State private var editing: SensorMark?
    @State private var label = ""

    var body: some View {
        List {
            VStack(spacing: Layout.contentGap) {
                HStack {
                    Spacer()
                    Button(action: openDevice) {
                        DeviceIcon().stroke(Palette.muted, lineWidth: 1.5).frame(width: 27, height: 21)
                            .overlay(alignment: .topTrailing) {
                                StatusDot(level: collector.bluetoothState == .poweredOn && collector.state == "Streaming" ? (collector.contact == false ? .fair : .good) : .none).offset(x: 3, y: -3)
                            }.frame(width: 44, height: 44)
                    }.buttonStyle(.plain).accessibilityLabel("Open Device").accessibilityIdentifier("open-device")
                }
                TimelineView(.periodic(from: .now, by: 1)) { context in
                    let end = DisplayClock.date(context.date).timeIntervalSince1970
                    let points = makePoints(end: end)
                    let value = selection.flatMap { stamp -> Double? in
                        guard let point = points.min(by: { abs($0.time - stamp) < abs($1.time - stamp) }), abs(point.time - stamp) <= 2.5 else { return nil }
                        return point.value }
                    let fresh = collector.bluetoothState == .poweredOn && collector.signals.heartRateFresh && collector.heartRate != nil && collector.state == "Streaming"
                    VStack(spacing: 0) {
                        BeatingHeart(signals: collector.signals, bpm: collector.heartRate, live: fresh).frame(height: Layout.heartHeight)
                        HStack(alignment: .firstTextBaseline, spacing: 5) {
                            Text(formatValue(selection == nil ? (fresh ? collector.heartRate.map(Double.init) : nil) : value))
                                .font(.system(size: Layout.heartRateSize, weight: .semibold, design: .rounded)).tracking(-2).monospacedDigit()
                            Text("bpm").font(.system(size: 16)).foregroundStyle(Palette.muted)
                        }
                        if let notice = bluetoothNotice {
                            VStack(spacing: 8) {
                                Label(notice.title, systemImage: "antenna.radiowaves.left.and.right.slash")
                                    .font(.system(size: 15, weight: .medium)).foregroundStyle(Palette.muted)
                                    .accessibilityIdentifier("bluetooth-status")
                                Text(notice.detail).font(.system(size: 13)).foregroundStyle(Palette.muted)
                                if notice.settings {
                                    Button("Open Settings") {
                                        if let url = URL(string: UIApplication.openSettingsURLString) { UIApplication.shared.open(url) }
                                    }.buttonStyle(ActionButtonStyle(compact: true)).padding(.top, 10)
                                        .accessibilityIdentifier("bluetooth-settings")
                                }
                            }.padding(.top, 22)
                        } else if let selection {
                            Text(Date(timeIntervalSince1970: selection).formatted(date: .omitted, time: .standard))
                                .font(.system(size: 11)).foregroundStyle(Palette.muted).padding(.top, 8)
                        } else if let contactText {
                            Text(contactText).font(.system(size: 12)).foregroundStyle(Palette.muted).padding(.top, 8)
                        }
                        Text("Last 10 minutes").font(.system(size: 12)).foregroundStyle(Palette.muted)
                            .frame(maxWidth: .infinity, alignment: .leading).padding(.top, 28)
                        SignalPlot(points: points, start: end - 600, end: end, color: Palette.heart, marks: collector.marks, onSelectMark: editMark, showExtremes: true, selectionTolerance: 2.5, selection: $selection)
                            .frame(height: Layout.heartRateChartHeight).padding(.top, 8)
                    }
                }
                VStack(spacing: 3) {
                    SectionTitle(title: NowContent.ecg.title, info: NowContent.ecg.info)
                    ECGPlot(signals: collector.signals).frame(height: Layout.chartHeight)
                }
                MovementView(signals: collector.signals)
                VStack(alignment: .leading, spacing: 4) {
                    HStack {
                        SectionTitle(title: NowContent.events.title, info: NowContent.events.info)
                        Button { collector.addMark() } label: { Label("Add", systemImage: "plus").font(.system(size: 14, weight: .semibold)).frame(minHeight: 44) }
                            .buttonStyle(.plain).disabled(collector.state != "Streaming").accessibilityIdentifier("add-mark")
                    }
                    if collector.marks.isEmpty { Text("Save a moment to find it later.").font(.system(size: 13)).foregroundStyle(Palette.muted) }
                    if let error = collector.markError { Text(error).font(.system(size: 13)).foregroundStyle(Palette.bad) }
                }
            }
            .listRowInsets(EdgeInsets(top: 12, leading: 24, bottom: 4, trailing: 24))
            .listRowSeparator(.hidden).listRowBackground(Palette.canvas)
            ForEach(collector.marks.suffix(10).reversed()) { mark in
                Button { editMark(mark) } label: {
                    HStack(spacing: 12) {
                        Circle().fill(Palette.faint).frame(width: 5, height: 5)
                        Text(mark.displayLabel).lineLimit(1)
                        Spacer()
                        Text(mark.time.formatted(date: .omitted, time: .shortened)).foregroundStyle(Palette.muted).monospacedDigit()
                    }.font(.system(size: 14)).padding(.vertical, 12).contentShape(Rectangle())
                }.buttonStyle(.plain).accessibilityIdentifier("mark-" + mark.id)
                    .swipeActions(edge: .trailing, allowsFullSwipe: false) {
                        Button("Delete", systemImage: "trash", role: .destructive) { collector.deleteMark(mark.id) }.tint(.red)
                    }
                    .listRowInsets(EdgeInsets(top: 0, leading: 24, bottom: 0, trailing: 24))
                    .listRowSeparator(.hidden).listRowBackground(Palette.canvas)
            }
        }
        .listStyle(.plain).scrollContentBackground(.hidden)
        .contentMargins(.bottom, 32, for: .scrollContent)
        .background(Palette.canvas).foregroundStyle(Palette.ink)
        .alert("Rename event", isPresented: Binding(get: { editing != nil }, set: { if !$0 { editing = nil } })) {
            TextField("Label", text: $label)
            Button("Save") { if let editing { collector.renameMark(editing.id, to: label) }; editing = nil }
            Button("Cancel", role: .cancel) { editing = nil }
        } message: {
            if let editing { Text(editing.time.formatted(date: .abbreviated, time: .standard)) }
        }
    }

    private func editMark(_ mark: SensorMark) {
        label = mark.displayLabel
        editing = mark
    }

    private var contactText: String? {
        guard collector.state == "Streaming" else { return collector.running ? collector.state : "Connect your H10 in Device" }
        if collector.contact == false { return "Check strap contact" }
        if !collector.signals.heartRateFresh || collector.heartRate == nil { return "Waiting for heart rate" }
        return nil
    }

    private var bluetoothNotice: (title: String, detail: String, settings: Bool)? {
        switch collector.bluetoothState {
        case .poweredOff: ("Bluetooth off", "Turn it on in Control Center.", false)
        case .unauthorized: ("Bluetooth access off", "Allow Bluetooth for Training Sensors.", true)
        default: nil
        }
    }

    private func makePoints(end: Double) -> [PlotPoint] {
        let trace = collector.signals.heartRate
        let offset = end - DisplayClock.uptime
        let start = trace.index(after: DisplayClock.uptime - 600)
        var points: [PlotPoint] = []
        for index in start..<trace.times.count {
            if index > start, trace.times[index] - trace.times[index - 1] > 3 { points.append(PlotPoint(time: trace.times[index] + offset - 0.1, value: nil)) }
            points.append(PlotPoint(time: trace.times[index] + offset, value: trace.values[0][index]))
        }
        return points
    }
}
