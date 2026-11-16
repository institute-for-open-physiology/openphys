import SwiftUI
import H10Kit

struct ContentView: View {
    @ObservedObject var model: AppModel
    @ObservedObject var collector: H10Collector
    @State private var tab = MobileScreen.now
    @State private var serverSheet: ServerSheet?
    @State private var linkError: String?

    var body: some View {
        TabView(selection: $tab) {
            ForEach(MobileScreen.allCases, id: \.rawValue) { screen in
                Group {
                    switch screen {
                    case .now: NowView(collector: collector, openDevice: { tab = .device })
                    case .history: HistoryView(collector: collector)
                    case .device: DeviceView(collector: collector).disabled(model.preparingQueues)
                    case .sync: SyncView(model: model, sheet: $serverSheet)
                    }
                }
                .tabItem {
                    Label { Text(screen.title) } icon: {
                        switch screen {
                        case .now: outline("heart")
                        case .history: Image(systemName: "clock.arrow.circlepath")
                        case .device: Image(uiImage: DeviceIcon.image)
                        case .sync: outline("cloud")
                        }
                    }
                }.tag(screen)
            }
        }
        .tint(Palette.ink)
        .toolbarBackground(Palette.canvas, for: .tabBar)
        .toolbarBackground(.visible, for: .tabBar)
        .sheet(item: $serverSheet) { sheet in
            if sheet.showDetails, let remote = sheet.remote {
                RemoteDetail(model: model, id: remote.id)
            } else {
                RemoteLogin(model: model, remote: sheet.remote, initialLink: sheet.link)
            }
        }
        .onOpenURL { url in
            guard let connection = EnrollmentLink.parseAppURL(url) else {
                linkError = "This connection link is invalid. Ask for a new link."
                return
            }
            let remote = model.remotes.first { $0.origin.lowercased() == connection.origin.absoluteString.lowercased() }
            guard remote.map({ !model.signedIn.contains($0.id) }) ?? true else {
                linkError = "This server is already connected. Sign out before using another connection link."
                return
            }
            tab = .sync
            serverSheet = ServerSheet(remote: remote, link: connection.link)
        }
        .alert("Cannot open connection link", isPresented: Binding(get: { linkError != nil }, set: { if !$0 { linkError = nil } })) {
            Button("OK", role: .cancel) { linkError = nil }
        } message: {
            Text(linkError ?? "")
        }
        .alert("Storage running low", isPresented: $model.showingStorageWarning) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Free up space to keep recording. Unsent readings stay on this iPhone.")
        }
        .onAppear {
            #if DEBUG
            let args = ProcessInfo.processInfo.arguments
            if let index = args.firstIndex(of: "-tab"), args.indices.contains(index + 1) { tab = MobileScreen(rawValue: Int(args[index + 1]) ?? 0) ?? .now }
            #endif
        }
    }

    private func outline(_ name: String) -> Image {
        Image(uiImage: (UIImage(systemName: name) ?? UIImage()).withRenderingMode(.alwaysTemplate))
    }
}
