import AppKit
import SwiftUI
import Darwin

@MainActor final class RecorderModel: ObservableObject {
    @Published var state = RecorderState()
    @Published var busy = false
    @Published var selectedVolume = ""
    @Published var rate = 32000
    @Published var duration = 60.0
    @Published var contact = false
    @Published var selectedSession = ""
    @Published var link = ""
    @Published var replaceLink = false
    @Published var stopping = false
    private var process: Process?
    private var activeCommand = ""
    private var generation = UUID()
    private var liveness: HelperLiveness?
    private var uploadCancelled = false
    private var accessCancelled = false
    static let cloud = URL(string: "https://sensors-poc.vineyard-finance.com")!

    func start(_ command: String, arguments: [String] = [], input: [String: String]? = nil) {
        guard !busy else { return }
        do {
            guard let resources = Bundle.main.resourceURL else { throw RecorderError.missingRuntime }
            let runtimeData = try Data(contentsOf: resources.appendingPathComponent("runtime.json"))
            guard let config = try JSONSerialization.jsonObject(with: runtimeData) as? [String: String],
                  let python = config["python"], python.hasPrefix("/"), FileManager.default.isExecutableFile(atPath: python) else { throw RecorderError.missingRuntime }
            let helper = resources.appendingPathComponent("helpers/seca_macos_bridge.py")
            guard FileManager.default.fileExists(atPath: helper.path) else { throw RecorderError.missingRuntime }
            let child = Process(), output = Pipe(), errors = Pipe(), stdin = Pipe()
            child.executableURL = URL(fileURLWithPath: python)
            child.arguments = ["-s", "-u", helper.path, command] + arguments
            child.standardOutput = output; child.standardError = errors; child.standardInput = stdin
            var inputData = Data()
            if let input {
                inputData = try JSONSerialization.data(withJSONObject: input)
                inputData.append(10)
                guard inputData.count <= 16384 else { throw RecorderError.invalidEvent }
            }
            // The helper emits only redacted structured errors. Drain stderr so a
            // traceback cannot block cleanup; never display arbitrary payloads.
            errors.fileHandleForReading.readabilityHandler = { handle in _ = handle.availableData }
            try child.run()
            state.begin(command); busy = true; process = child; activeCommand = command
            uploadCancelled = false
            accessCancelled = false
            let now = ProcessInfo.processInfo.systemUptime
            liveness = HelperLiveness(command: command, started: now, lastProgress: now)
            let token = UUID(); generation = token
            DispatchQueue.global(qos: .userInitiated).async { [weak self] in
                var framing = StatusLines()
                var invalid = false
                while true {
                    let data = output.fileHandleForReading.availableData
                    if data.isEmpty { break }
                    if invalid { continue } // Still drain faulty producer through EOF.
                    do {
                        for row in try framing.feed(data) {
                            guard let value = try JSONSerialization.jsonObject(with: row) as? [String: Any] else { throw RecorderError.invalidEvent }
                            DispatchQueue.main.async { self?.receive(value, token: token) }
                        }
                    } catch {
                        invalid = true
                        DispatchQueue.main.async { self?.statusFailure(token: token) }
                    }
                }
                if invalid || framing.hasPartialLine {
                    DispatchQueue.main.async { self?.statusFailure(token: token) }
                    // A malformed UI stream cannot abandon USB cleanup.
                    if child.isRunning { _ = interruptSupervisor(pid: child.processIdentifier) }
                }
                child.waitUntilExit()
                errors.fileHandleForReading.readabilityHandler = nil
                DispatchQueue.main.async { self?.finished(child.terminationStatus, token: token) }
            }
            if !inputData.isEmpty { try stdin.fileHandleForWriting.write(contentsOf: inputData) }
            try stdin.fileHandleForWriting.close()
            supervise(token: token)
        } catch {
            state.error = error.localizedDescription; state.phase = "Needs attention"
            if let process, process.isRunning { _ = interruptSupervisor(pid: process.processIdentifier) }
        }
    }

    private func receive(_ event: [String: Any], token: UUID) {
        guard generation == token else { return }
        do {
            try state.apply(event)
            liveness?.lastProgress = ProcessInfo.processInfo.systemUptime
            if event["event"] as? String == "state" {
                if !state.volumes.contains(selectedVolume) { selectedVolume = state.volumes.first ?? "" }
                if !state.sessions.contains(where: { $0.name == selectedSession }) { selectedSession = state.sessions.first?.name ?? "" }
            }
            if ["session", "saved"].contains(event["event"] as? String ?? ""), let path = state.currentPath {
                selectedSession = URL(fileURLWithPath: path).lastPathComponent
            }
        } catch { statusFailure(token: token) }
    }
    private func statusFailure(token: UUID) {
        guard generation == token else { return }
        state.error = RecorderError.invalidEvent.localizedDescription
        state.phase = "Status error · stopping safely"
        if activeCommand == "upload" { cancelUpload() }
        else if activeCommand == "access" { cancelAccess() }
        else if activeCommand == "record" { stop() }
        else { process?.terminate() }
    }
    private func finished(_ code: Int32, token: UUID) {
        guard generation == token else { return }
        if uploadCancelled {
            state.isRecording = false
            state.phase = "Upload paused · immutable files retained for retry"
        } else if accessCancelled {
            state.accessVolume = nil; state.phase = "USB access cancelled · recorder was not started"
        } else { state.finished(exitCode: code) }
        busy = false; stopping = false; process = nil; liveness = nil
        if activeCommand == "record", let path = state.currentPath,
           !state.sessions.contains(where: { $0.path == path }) {
            let name = URL(fileURLWithPath: path).lastPathComponent
            state.sessions.insert(SavedRecording(name: name, path: path, closed: state.error == nil,
                                                 uploadable: state.currentUploadable,
                                                 endReason: state.error == nil ? "saved" : "inspect local files"), at: 0)
            selectedSession = name
        }
    }
    func record() {
        guard state.accessVolume == selectedVolume else { return }
        var args = ["--volume", selectedVolume, "--rate", String(rate), "--duration", String(duration)]
        if contact { args.append("--contact") }
        start("record", arguments: args)
    }
    func stop() {
        guard busy, activeCommand == "record", !stopping else { return }
        stopping = true; state.phase = "Stopping · waiting for standby and archive close…"
        if let process, process.isRunning, !interruptSupervisor(pid: process.processIdentifier) {
            state.error = "Could not signal the recording supervisor; keep the app open for cleanup."
        }
        let token = generation
        DispatchQueue.main.asyncAfter(deadline: .now() + 15) { [weak self] in
            guard let self, self.generation == token, self.busy else { return }
            self.state.error = "Stop has not completed. Local files are retained; device state is uncertain. Keep the app open for cleanup."
        }
    }
    func enroll() {
        let secret = link; link = ""
        start("enroll", arguments: replaceLink ? ["--replace-link"] : [], input: ["link": secret])
    }
    func reveal() {
        let selected = state.sessions.first(where: { $0.name == selectedSession })?.path ?? state.currentPath
        if let selected { NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: selected)]) }
    }
    var canStop: Bool { busy && activeCommand == "record" && !stopping }
    var recordingProcess: Bool { busy && activeCommand == "record" }
    var uploadingProcess: Bool { busy && activeCommand == "upload" }
    var accessProcess: Bool { busy && activeCommand == "access" }
    func cancelAccess() {
        guard accessProcess, !accessCancelled else { return }
        accessCancelled = true
        state.phase = "Cancelling USB access · recorder was not started…"
        terminateCancellableHelper()
    }
    func cancelUpload() {
        guard uploadingProcess, !uploadCancelled else { return }
        uploadCancelled = true
        state.phase = "Pausing upload · retaining files for retry…"
        terminateCancellableHelper()
    }
    private func terminateCancellableHelper() {
        guard ["upload", "access"].contains(activeCommand) else { return }
        process?.terminate()
        let token = generation
        DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
            guard let self, self.generation == token, self.busy, let process = self.process, process.isRunning else { return }
            // Only upload or read-only permission probing can reach this path.
            // Neither owns ECG acquisition. Never kill a recording supervisor.
            kill(process.processIdentifier, SIGKILL)
        }
    }
    private func supervise(token: UUID) {
        DispatchQueue.main.asyncAfter(deadline: .now() + 1) { [weak self] in
            guard let self, self.generation == token, self.busy else { return }
            if let action = self.liveness?.check(now: ProcessInfo.processInfo.systemUptime) {
                self.state.error = "Recorder helper stopped reporting progress; local files retained."
                if action == "stop_recording" { self.stop() }
                else if action == "cancel_upload" { self.cancelUpload() }
                else if action == "cancel_access" {
                    self.state.error = "USB permission timed out. Accept the macOS removable-volume prompt, or check Privacy & Security > Files and Folders."
                    self.cancelAccess()
                }
                else { self.process?.terminate() }
            }
            self.supervise(token: token)
        }
    }
}

@MainActor final class AppDelegate: NSObject, NSApplicationDelegate, NSWindowDelegate {
    weak var model: RecorderModel?
    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        guard let model, model.busy else { return .terminateNow }
        promptStop(model)
        return .terminateCancel
    }
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard let model, model.busy else { return true }
        promptStop(model)
        return false
    }
    private func promptStop(_ model: RecorderModel) {
        let alert = NSAlert()
        alert.messageText = model.recordingProcess ? "Finish the recording before closing" : "Wait for the current operation to finish"
        alert.informativeText = "Keep the app open until local files and the current operation finish."
        if model.recordingProcess {
            alert.addButton(withTitle: "Stop recording")
            alert.addButton(withTitle: "Keep recording")
            if alert.runModal() == .alertFirstButtonReturn { model.stop() }
        } else if model.uploadingProcess {
            alert.addButton(withTitle: "Pause upload")
            alert.addButton(withTitle: "Keep uploading")
            if alert.runModal() == .alertFirstButtonReturn { model.cancelUpload() }
        } else if model.accessProcess {
            alert.addButton(withTitle: "Cancel USB access")
            alert.addButton(withTitle: "Keep waiting")
            if alert.runModal() == .alertFirstButtonReturn { model.cancelAccess() }
        } else { alert.addButton(withTitle: "Keep app open"); alert.runModal() }
    }
}

struct RecorderView: View {
    @ObservedObject var model: RecorderModel
    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                VStack(alignment: .leading) {
                    Text("seca · USB ECG").font(.largeTitle.bold())
                    Text("Native developer app · recordings upload after Stop").foregroundStyle(.secondary)
                }
                Spacer()
                Button("Open Carolina") { NSWorkspace.shared.open(RecorderModel.cloud) }
            }
            GroupBox("Recorder") {
                VStack(alignment: .leading, spacing: 10) {
                    HStack {
                        Picker("USB recorder", selection: $model.selectedVolume) {
                            if model.state.volumes.isEmpty { Text("Connect and mount the seca").tag("") }
                            ForEach(model.state.volumes, id: \.self) { Text($0).tag($0) }
                        }.disabled(model.busy).onChange(of: model.selectedVolume) { _ in model.state.accessVolume = nil }
                        Button("Refresh") { model.start("state") }.disabled(model.busy)
                    }
                    HStack {
                        Picker("Requested rate", selection: $model.rate) {
                            ForEach([1000,2000,4000,8000,16000,32000], id: \.self) { Text("\($0 / 1000) kHz").tag($0) }
                        }.frame(width: 230)
                        Text("Duration (seconds)")
                        TextField("60", value: $model.duration, format: .number).frame(width: 90)
                        Toggle("Contact detection", isOn: $model.contact)
                    }.disabled(model.busy)
                    HStack {
                        Button("Allow USB Access") { model.start("access", arguments: ["--volume", model.selectedVolume]) }
                            .disabled(model.busy || model.selectedVolume.isEmpty)
                        if model.accessProcess { Button("Cancel access") { model.cancelAccess() } }
                        Button("Start recording") { model.record() }
                            .disabled(model.busy || model.selectedVolume.isEmpty || model.state.accessVolume != model.selectedVolume || !model.duration.isFinite || model.duration <= 0 || model.duration > 86400)
                        Button(model.stopping ? "Stopping…" : "Stop and save") { model.stop() }.disabled(!model.canStop)
                        Spacer()
                        Text("Serial: \(model.state.serial)").foregroundStyle(.secondary)
                    }
                }.padding(8)
            }
            GroupBox {
                VStack(alignment: .leading, spacing: 8) {
                    Text(model.state.phase).font(.headline)
                    Text("Confirmed: \(model.state.confirmedRate.map { "\($0 / 1000) kHz · gain 6" } ?? "awaiting readback") · contacts \(model.state.contact ? "enabled" : "disabled")")
                    Text(String(format: "%.1f s · %.1f KiB · %d groups · %d missing · %d decode/sequence errors", model.state.elapsed, Double(model.state.bytes) / 1024, model.state.groups, model.state.missing, model.state.decodeErrors))
                        .font(.system(.body, design: .monospaced))
                    if model.state.groups > 0 && model.state.constantGroups == model.state.groups {
                        Text("All observed groups are constant. Check electrodes and signal; this does not establish ECG morphology.").foregroundStyle(.orange)
                    }
                    if !model.state.contact { Text("Contact detection is disabled; zero flags do not prove electrode contact.").foregroundStyle(.secondary) }
                    if let error = model.state.error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
                }.frame(maxWidth: .infinity, alignment: .leading).padding(8)
            }
            GroupBox("Carolina") {
                VStack(alignment: .leading, spacing: 10) {
                    if let installation = model.state.installation {
                        Text("Mac installation: \(installation)").font(.caption).textSelection(.enabled)
                    } else {
                        Text("Create a Mac connection link in Carolina, then paste it here.")
                        HStack {
                            SecureField("One-use Mac connection link", text: $model.link)
                            Button("Connect Mac") { model.enroll() }.disabled(model.busy || model.link.isEmpty)
                        }
                        Toggle("Replace an expired pending link", isOn: $model.replaceLink).disabled(model.busy)
                    }
                    Picker("Saved recording", selection: $model.selectedSession) {
                        if model.state.sessions.isEmpty { Text("No recordings yet").tag("") }
                        ForEach(model.state.sessions) { Text("\($0.name) · \($0.endReason)").tag($0.name) }
                    }.disabled(model.busy)
                    HStack {
                        Button("Upload / retry selected") { model.start("upload", arguments: ["--session", model.selectedSession]) }
                            .disabled(model.busy || model.state.installation == nil || !(model.state.sessions.first(where: { $0.name == model.selectedSession })?.uploadable ?? false))
                        Button("Reveal recording") { model.reveal() }.disabled(model.selectedSession.isEmpty)
                        if model.uploadingProcess { Button("Pause upload") { model.cancelUpload() } }
                        Spacer()
                        Text("Acknowledged \(model.state.acknowledged) / \(model.state.batches) batches")
                    }
                    Text("Raw recordings and upload files stay on this Mac. Rate selection does not establish calibration, bandwidth or pacemaker detection.").font(.caption).foregroundStyle(.secondary)
                }.padding(8)
            }
        }.padding(22).frame(minWidth: 850, minHeight: 640)
    }
}

@main struct SecaRecorderApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var delegate
    @StateObject private var model = RecorderModel()
    var body: some Scene {
        Window("seca USB ECG", id: "recorder") {
            RecorderView(model: model).onAppear {
                delegate.model = model
                DispatchQueue.main.async {
                    NSApplication.shared.windows.first?.delegate = delegate
                    NSApplication.shared.activate(ignoringOtherApps: true)
                }
                if !model.busy { model.start("state") }
            }
        }.defaultSize(width: 900, height: 700)
    }
}
