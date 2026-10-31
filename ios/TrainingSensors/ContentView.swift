import SwiftUI

struct ContentView: View {
    @ObservedObject var model: AppModel
    @ObservedObject var collector: H10Collector
    @State private var tab = MobileScreen.now

    var body: some View {
        TabView(selection: $tab) {
            ForEach(MobileScreen.allCases, id: \.rawValue) { screen in
                Group {
                    switch screen {
                    case .now: NowView(collector: collector, openDevice: { tab = .device })
                    case .history: HistoryView(collector: collector)
                    case .device: DeviceView(collector: collector).disabled(model.preparingQueues)
                    case .sync: SyncView(model: model)
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
