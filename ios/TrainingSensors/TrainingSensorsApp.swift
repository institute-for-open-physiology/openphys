import SwiftUI

@main
struct TrainingSensorsApp: App {
    @StateObject private var model = AppModel()
    @Environment(\.scenePhase) private var phase

    var body: some Scene {
        WindowGroup {
            ContentView(model: model, collector: model.collector)
                .preferredColorScheme(DisplayClock.appearance)
                .task(id: phase) {
                    guard phase == .active else { return }
                    while !Task.isCancelled {
                        await model.checkStorage()
                        do { try await Task.sleep(for: .seconds(60)) } catch { return }
                    }
                }
                .onChange(of: phase) { _, phase in
                    if phase == .active { model.resume() }
                    if phase != .active { model.collector.signals.history.flush() }
                }
        }
    }
}
