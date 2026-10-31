import SwiftUI
import UIKit

enum Level {
    case good, fair, bad, none
    var color: Color {
        switch self { case .good: Palette.up; case .fair: Palette.warn; case .bad: Palette.bad; case .none: Palette.faint }
    }
    var label: String {
        switch self { case .good: "Good"; case .fair: "Low"; case .bad: "Poor"; case .none: "Unknown" }
    }
    static func battery(_ value: Int?) -> Level {
        guard let value else { return .none }
        return value <= 15 ? .bad : value <= 30 ? .fair : .good
    }
    static func signal(_ value: Int?) -> Level {
        guard let value else { return .none }
        return value < -80 ? .bad : value < -65 ? .fair : .good
    }
}

struct StatusDot: View {
    let level: Level
    var body: some View { Circle().fill(level.color).frame(width: 6, height: 6).accessibilityLabel(level.label) }
}

struct ActionButtonStyle: ButtonStyle {
    var destructive = false
    var compact = false
    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.isEnabled) private var enabled

    func makeBody(configuration: Configuration) -> some View {
        let foreground: Color = colorScheme == .dark ? .black : .white
        let background: Color = colorScheme == .dark ? .white : .black
        configuration.label
            .font(.system(size: 15, weight: .semibold))
            .frame(maxWidth: compact ? nil : .infinity, minHeight: Layout.buttonHeight)
            .padding(.horizontal, 18)
            .foregroundStyle(destructive ? Palette.bad : foreground)
            .background(destructive ? Palette.bad.opacity(0.12) : background,
                        in: RoundedRectangle(cornerRadius: Layout.buttonRadius))
            .opacity(enabled ? (configuration.isPressed ? 0.7 : 1) : 0.4)
    }
}

struct SectionTitle: View {
    let title: String
    var info: String? = nil
    var value: String? = nil
    var body: some View {
        HStack(alignment: .center, spacing: 5) {
            Text(title).font(.system(size: Layout.sectionTitleSize, weight: .semibold)).accessibilityIdentifier("section-" + title)
            if let info { InfoButton(title: title, text: info) }
            Spacer(minLength: 8)
            if let value { Text(value).font(.system(size: 14, weight: .medium)).monospacedDigit() }
        }
    }
}

struct InfoButton: View {
    let title: String
    let text: String
    @State private var presented = false
    var body: some View {
        Button { presented = true } label: {
            Image(systemName: "info.circle").font(.system(size: 13)).foregroundStyle(Palette.muted)
                .frame(width: 32, height: 44)
        }
        .buttonStyle(.plain)
        .accessibilityLabel("About " + title)
        .popover(isPresented: $presented, arrowEdge: .bottom) {
            VStack(alignment: .leading, spacing: 8) {
                Text(title).font(.headline)
                Text(text).font(.subheadline).foregroundStyle(Palette.muted).fixedSize(horizontal: false, vertical: true)
            }
            .padding(20).frame(idealWidth: 280, maxWidth: 300)
            .presentationCompactAdaptation(.popover)
        }
    }
}

struct Page<Content: View>: View {
    var spacing: CGFloat = Layout.sectionGap
    @ViewBuilder var content: Content
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: spacing) { content }
                .padding(.horizontal, Layout.pageInset).padding(.top, 12).padding(.bottom, 32)
        }
        .background(Palette.canvas).foregroundStyle(Palette.ink)
    }
}

struct PageTitle: View {
    let title: String
    var body: some View { Text(title).font(.system(size: Layout.titleSize, weight: .bold)).tracking(-0.6) }
}

struct DeviceIcon: Shape {
    func path(in rect: CGRect) -> Path {
        var path = Path()
        path.addRoundedRect(in: CGRect(x: rect.minX + rect.width * 0.17, y: rect.midY - rect.height * 0.29,
                                      width: rect.width * 0.66, height: rect.height * 0.58), cornerSize: CGSize(width: rect.height * 0.27, height: rect.height * 0.27))
        path.move(to: CGPoint(x: rect.minX, y: rect.midY))
        path.addLine(to: CGPoint(x: rect.minX + rect.width * 0.17, y: rect.midY))
        path.move(to: CGPoint(x: rect.minX + rect.width * 0.83, y: rect.midY))
        path.addLine(to: CGPoint(x: rect.maxX, y: rect.midY))
        return path
    }
    static let image: UIImage = UIGraphicsImageRenderer(size: CGSize(width: 28, height: 24)).image { context in
        context.cgContext.setStrokeColor(UIColor.label.cgColor)
        context.cgContext.setLineWidth(1.8)
        context.cgContext.setLineCap(.round)
        context.cgContext.addPath(DeviceIcon().path(in: CGRect(x: 1, y: 1, width: 26, height: 22)).cgPath)
        context.cgContext.strokePath()
    }.withRenderingMode(.alwaysTemplate)
}

func formatValue(_ value: Double?, unit: String = "") -> String {
    guard let value, value.isFinite else { return "--" }
    return "\(Int(value.rounded()))" + (unit.isEmpty ? "" : " " + unit)
}
