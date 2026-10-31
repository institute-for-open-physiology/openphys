import H10Kit
import SwiftUI
import WatchConnectivity

@MainActor
final class WatchModel: NSObject, ObservableObject, WCSessionDelegate {
    @Published private(set) var snapshot: WatchSnapshot?
    @Published private(set) var pending: [SensorMark] = []
    @Published private(set) var problem: String?
    private(set) var queueReadable = true
    private var requestInFlight = false
    private let queueURL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0].appendingPathComponent("pending-marks.json")

    override init() {
        super.init()
        if FileManager.default.fileExists(atPath: queueURL.path) {
            do { pending = try JSONDecoder().decode([SensorMark].self, from: Data(contentsOf: queueURL)) }
            catch { problem = "Cannot open saved events"; queueReadable = false }
        }
        WCSession.default.delegate = self
        WCSession.default.activate()
    }

    private func persist(_ marks: [SensorMark]) throws {
        try FileManager.default.createDirectory(at: queueURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try JSONEncoder().encode(marks).write(to: queueURL, options: .atomic)
        pending = marks
        problem = nil
    }

    func refresh() {
        let session = WCSession.default
        guard session.activationState == .activated else { return }
        if session.isReachable && !requestInFlight {
            requestInFlight = true
            session.sendMessage(["requestSnapshot": true], replyHandler: { packet in
                Task { @MainActor in self.requestInFlight = false; self.receive(packet) }
            }, errorHandler: { _ in Task { @MainActor in self.requestInFlight = false } })
        }
        sendPending()
    }

    private func sendPending() {
        let session = WCSession.default
        guard session.activationState == .activated else { return }
        let queued = Set(session.outstandingUserInfoTransfers.compactMap { $0.userInfo["id"] as? String })
        for mark in pending {
            guard let data = try? JSONEncoder().encode(mark) else { continue }
            let packet: [String: Any] = ["mark": data, "id": mark.id]
            if !queued.contains(mark.id) { session.transferUserInfo(packet) }
            if session.isReachable {
                session.sendMessage(packet, replyHandler: { packet in Task { @MainActor in self.receive(packet) } }, errorHandler: { _ in })
            }
        }
    }

    private func receive(_ packet: [String: Any]) {
        if let data = packet["snapshot"] as? Data, let value = try? JSONDecoder().decode(WatchSnapshot.self, from: data),
           snapshot == nil || value.sentAt > snapshot!.sentAt { snapshot = value }
        if let id = packet["received"] as? String, pending.contains(where: { $0.id == id }) {
            do {
                try persist(pending.filter { $0.id != id })
                for transfer in WCSession.default.outstandingUserInfoTransfers where transfer.userInfo["id"] as? String == id { transfer.cancel() }
            } catch { problem = "Could not update saved events" }
        }
    }

    nonisolated func session(_ session: WCSession, activationDidCompleteWith activationState: WCSessionActivationState, error: Error?) {
        Task { @MainActor in self.refresh() }
    }
    nonisolated func sessionReachabilityDidChange(_ session: WCSession) { Task { @MainActor in self.refresh() } }
    nonisolated func session(_ session: WCSession, didReceiveMessage message: [String: Any]) {
        Task { @MainActor in self.receive(message) }
    }
    nonisolated func session(_ session: WCSession, didReceiveUserInfo userInfo: [String: Any]) {
        Task { @MainActor in self.receive(userInfo) }
    }
}
