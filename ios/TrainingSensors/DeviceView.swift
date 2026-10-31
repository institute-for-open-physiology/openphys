import SwiftUI

struct DeviceView: View {
    @ObservedObject var collector: H10Collector
    private var connected: Bool { collector.state == "Streaming" }

    var body: some View {
        Page {
            PageTitle(title: "Device")
            VStack(spacing: 18) {
                H10Illustration().frame(height: 110).padding(.vertical, 6)
                VStack(spacing: 8) {
                    Text("Polar H10").font(.system(size: 23, weight: .semibold))
                    HStack(spacing: 7) {
                        StatusDot(level: connected ? .good : .none)
                        Text(connected ? "Connected" : collector.running ? collector.state : "Disconnected")
                            .font(.system(size: 12)).foregroundStyle(Palette.muted)
                    }
                }
                if !connected {
                    Button { collector.start() } label: {
                        HStack(spacing: 10) {
                            if collector.running { ProgressView() }
                            Text(collector.running ? "Connecting…" : "Connect")
                        }
                    }
                    .buttonStyle(ActionButtonStyle()).disabled(collector.running)
                    .accessibilityIdentifier("connect-device")
                }
            }.frame(maxWidth: .infinity)
            VStack(spacing: 0) {
                status("battery.75percent", "Battery", collector.battery.map { "\($0)%" } ?? "--", Level.battery(collector.battery))
                Divider().overlay(Palette.line)
                status("antenna.radiowaves.left.and.right", "Signal", collector.rssi.map { "\($0) dBm" } ?? "--", Level.signal(collector.rssi))
                Divider().overlay(Palette.line)
                status("waveform.path.ecg", "Skin contact", collector.contact.map { $0 ? "Detected" : "Lost" } ?? "--", collector.contact.map { $0 ? .good : .bad } ?? .none)
                Divider().overlay(Palette.line)
                status("clock", "Clock", connected ? (collector.clockSynced ? "Synced" : "Not synced") : "--", connected ? (collector.clockSynced ? .good : .fair) : .none)
            }.padding(.horizontal, 18).background(Palette.surface, in: RoundedRectangle(cornerRadius: Layout.cardRadius))
            if !connected && !collector.detail.isEmpty { Text(collector.detail).font(.system(size: 13)).foregroundStyle(Palette.muted) }
            VStack(alignment: .leading, spacing: 18) {
                SectionTitle(title: DeviceContent.timeline.title)
                Text("Today").font(.system(size: 11, weight: .medium)).foregroundStyle(Palette.muted)
                if collector.log.isEmpty { Text("Connection events appear here.").font(.system(size: 13)).foregroundStyle(Palette.muted) }
                ForEach(Array(collector.log.suffix(15).reversed().enumerated()), id: \.element.id) { index, event in
                    timeline(event, last: index == min(14, collector.log.count - 1))
                }
            }
            if collector.running {
                Button(connected ? "Disconnect" : "Cancel connection", role: .destructive) { collector.stop() }
                    .buttonStyle(ActionButtonStyle(destructive: true))
                    .accessibilityIdentifier("disconnect-device")
            }
        }
    }

    private func status(_ symbol: String, _ name: String, _ value: String, _ level: Level) -> some View {
        HStack(spacing: 12) {
            Image(systemName: symbol).font(.system(size: 15)).foregroundStyle(Palette.muted).frame(width: 20)
            Text(name).foregroundStyle(Palette.muted)
            Spacer()
            Text(value).monospacedDigit()
            StatusDot(level: level).padding(.leading, 4)
        }.font(.system(size: 14)).padding(.vertical, 18).accessibilityElement(children: .combine)
    }

    private func timeline(_ event: LogLine, last: Bool) -> some View {
        let parts = event.text.components(separatedBy: ": ")
        return HStack(alignment: .top, spacing: 15) {
            Text(event.time.formatted(date: .omitted, time: .shortened))
                .font(.system(size: 11)).monospacedDigit().foregroundStyle(Palette.muted).frame(width: 42, alignment: .leading).padding(.top, 2)
            VStack(spacing: 0) {
                Circle().fill(Palette.faint).frame(width: 5, height: 5).padding(.top, 6)
                if !last { Rectangle().fill(Palette.line).frame(width: 1).padding(.top, 6).padding(.bottom, -18) }
            }.frame(width: 5)
            VStack(alignment: .leading, spacing: 5) {
                Text(parts[0] == "Streaming" ? "Connected" : parts[0]).font(.system(size: 13, weight: .medium))
                if parts.count > 1 { Text(parts.dropFirst().joined(separator: ": ")).font(.system(size: 11)).foregroundStyle(Palette.muted).fixedSize(horizontal: false, vertical: true) }
            }.frame(maxWidth: .infinity, alignment: .leading).padding(.bottom, 6)
        }.fixedSize(horizontal: false, vertical: true)
    }
}

private struct H10Illustration: View {
    var body: some View {
        GeometryReader { geometry in
            ZStack {
                Capsule().fill(Palette.raised.opacity(0.45)).frame(width: geometry.size.width * 0.86, height: 35)
                Capsule().fill(LinearGradient(colors: [Color(white: 0.19), Color(white: 0.055)], startPoint: .topLeading, endPoint: .bottomTrailing))
                    .overlay(Capsule().strokeBorder(Color.white.opacity(0.08), lineWidth: 1))
                    .frame(width: 158, height: 94)
                    .shadow(color: .black.opacity(0.2), radius: 16, y: 10)
                VStack(spacing: 7) {
                    Text("POLAR").font(.system(size: 17, weight: .bold)).tracking(3).foregroundStyle(Color(white: 0.55))
                    Text("H10").font(.system(size: 9, weight: .medium)).tracking(2).foregroundStyle(Color(white: 0.33))
                }
            }.frame(maxWidth: .infinity, maxHeight: .infinity)
        }.accessibilityLabel("Polar H10 chest sensor")
    }
}
