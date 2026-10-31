import H10Kit
import QuartzCore
import WatchConnectivity

@MainActor
final class WatchCompanion: NSObject, WCSessionDelegate {
    private let collector: H10Collector

    init(collector: H10Collector) {
        self.collector = collector
        super.init()
        guard WCSession.isSupported() else { return }
        WCSession.default.delegate = self
        WCSession.default.activate()
    }

    private func snapshot() -> [String: Any] {
        let now = Date()
        let clock = CACurrentMediaTime(), signals = collector.signals
        var ecg: WatchECG?
        if signals.ecgFresh, let measured = collector.ecgAt {
            let trace = signals.ecg, first = trace.index(after: clock - 6)
            ecg = WatchECG(measuredAt: measured, offsets: trace.times[first...].map { $0 - clock },
                           microvolts: trace.values[0][first...].map { Int32($0) },
                           lower: signals.ecgScale.lower, upper: signals.ecgScale.upper)
        }
        let value = WatchSnapshot(sentAt: now, measuredAt: collector.hrAt, heartRate: collector.heartRate,
                                  connected: collector.state == "Streaming", contact: collector.contact, battery: collector.battery, ecg: ecg)
        guard let data = try? JSONEncoder().encode(value) else { return [:] }
        return ["snapshot": data]
    }

    private func publish() {
        let session = WCSession.default
        guard session.activationState == .activated, session.isReachable else { return }
        session.sendMessage(snapshot(), replyHandler: nil, errorHandler: { _ in })
    }

    private func receive(_ message: [String: Any], reply: (([String: Any]) -> Void)?) {
        if let data = message["mark"] as? Data,
           let mark = try? JSONDecoder().decode(SensorMark.self, from: data), collector.receiveWatchMark(mark) {
            let receipt: [String: Any] = ["received": mark.id]
            if let reply { reply(receipt) }
            else { WCSession.default.transferUserInfo(receipt) }
        } else if message["requestSnapshot"] as? Bool == true { reply?(snapshot()) }
        else { reply?([:]) }
    }

    nonisolated func session(_ session: WCSession, activationDidCompleteWith activationState: WCSessionActivationState, error: Error?) {
        Task { @MainActor in if error == nil { self.publish() } }
    }
    nonisolated func sessionReachabilityDidChange(_ session: WCSession) { Task { @MainActor in self.publish() } }
    nonisolated func sessionDidBecomeInactive(_ session: WCSession) {}
    nonisolated func sessionDidDeactivate(_ session: WCSession) { session.activate() }
    nonisolated func session(_ session: WCSession, didReceiveMessage message: [String: Any], replyHandler: @escaping ([String: Any]) -> Void) {
        Task { @MainActor in self.receive(message, reply: replyHandler) }
    }
    nonisolated func session(_ session: WCSession, didReceiveUserInfo userInfo: [String: Any]) {
        Task { @MainActor in self.receive(userInfo, reply: nil) }
    }
}
