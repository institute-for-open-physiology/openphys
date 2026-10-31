import HealthKit
import H10Kit
import SwiftUI

@MainActor
final class HealthExport: ObservableObject {
    @Published private(set) var enabled = false
    @Published private(set) var authorizing = false
    @Published private(set) var error: String?
    let available: Bool
    private let store = HKHealthStore()
    private let heartRate = HKQuantityType(.heartRate)
    private var queue: HealthExportQueue?
    private var saving = false
    private var scheduledSave: Task<Void, Never>?
    private static let preference = "healthExportEnabled"

    init() {
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("-demo") { available = false; return }
        #endif
        available = HKHealthStore.isHealthDataAvailable()
        guard available else { return }
        enabled = UserDefaults.standard.bool(forKey: Self.preference)
        do {
            let directory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            queue = try HealthExportQueue(url: directory.appendingPathComponent("health-export.json"))
        } catch { self.error = "Could not open pending Health readings."; updateEnabled(false) }
    }

    func setEnabled(_ value: Bool) async {
        guard available, !authorizing else { return }
        if !value { updateEnabled(false); error = nil; return }
        guard queue != nil else { error = "Could not open pending Health readings."; return }
        authorizing = true
        defer { authorizing = false }
        do {
            try await store.requestAuthorization(toShare: [heartRate], read: [])
            guard store.authorizationStatus(for: heartRate) == .sharingAuthorized else {
                error = "Allow Training Sensors to save heart rate in Health."
                return
            }
            error = nil
            updateEnabled(true)
            flush()
        } catch { self.error = "Could not request Health access. Try again." }
    }

    func resume() {
        guard available, enabled else { return }
        guard checkAuthorization() else { return }
        scheduledSave?.cancel(); scheduledSave = nil
        flush()
    }

    func record(_ sample: H10Protocol.HeartRateSample, at time: Date, serial: String?) {
        guard available, enabled, let queue, sample.hasHeartRate, checkAuthorization() else { return }
        do { try queue.append(sample, at: time, serial: serial) }
        catch {
            self.error = (error as? H10Error)?.message ?? "Could not save pending Health readings."
            updateEnabled(false)
            return
        }
        scheduleSave()
    }

    private func checkAuthorization() -> Bool {
        guard store.authorizationStatus(for: heartRate) == .sharingAuthorized else {
            updateEnabled(false)
            error = "Allow Training Sensors to save heart rate in Health."
            return false
        }
        return true
    }

    private func updateEnabled(_ value: Bool) {
        enabled = value
        UserDefaults.standard.set(value, forKey: Self.preference)
        if !value { scheduledSave?.cancel(); scheduledSave = nil }
    }

    private func scheduleSave() {
        guard enabled, !saving, scheduledSave == nil, queue?.readings.isEmpty == false else { return }
        scheduledSave = Task { [weak self] in
            do { try await Task.sleep(for: .seconds(30)) } catch { return }
            guard let self else { return }
            self.scheduledSave = nil
            self.flush()
        }
    }

    private func flush() {
        guard enabled, !saving, scheduledSave == nil, let queue, !queue.readings.isEmpty, checkAuthorization() else { return }
        let batch = Array(queue.readings.prefix(120))
        let samples = batch.map { reading in
            let device = HKDevice(name: "Polar H10", manufacturer: "Polar", model: "H10", hardwareVersion: nil,
                                  firmwareVersion: nil, softwareVersion: nil, localIdentifier: reading.serial, udiDeviceIdentifier: nil)
            return HKQuantitySample(type: heartRate, quantity: HKQuantity(unit: .count().unitDivided(by: .minute()), doubleValue: Double(reading.bpm)),
                                    start: reading.time, end: reading.time, device: device,
                                    metadata: [HKMetadataKeySyncIdentifier: reading.id, HKMetadataKeySyncVersion: 1])
        }
        saving = true
        Task {
            do {
                try await store.save(samples)
                try queue.acknowledge(Set(batch.map(\.id)))
                saving = false
                if enabled { error = nil; scheduleSave() }
            } catch {
                saving = false
                guard enabled else { return }
                self.error = "Could not save to Health. Retrying."
                scheduleSave()
            }
        }
    }
}

struct HealthExportRow: View {
    @ObservedObject var health: HealthExport

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Toggle(isOn: Binding(get: { health.enabled }, set: { value in Task { await health.setEnabled(value) } })) {
                HStack(spacing: 16) {
                    Image(systemName: "heart").font(.system(size: 23, weight: .light)).foregroundStyle(Palette.muted).frame(width: 26)
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Apple Health").font(.system(size: 16, weight: .semibold))
                        Text("Heart rate").font(.system(size: 12)).foregroundStyle(Palette.muted)
                    }
                }
            }
            .tint(Palette.up).frame(height: 108)
            .disabled(!health.available || health.authorizing)
            .accessibilityLabel("Save heart rate to Apple Health").accessibilityIdentifier("health-export")
            if let error = health.error { Text(error).font(.footnote).foregroundStyle(Palette.bad).padding(.leading, 42) }
        }
    }
}
