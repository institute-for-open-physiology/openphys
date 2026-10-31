import Foundation
import Darwin

func interruptSupervisor(pid: Int32, sender: (Int32, Int32) -> Int32 = { kill($0, $1) }) -> Bool {
    // A non-positive PID would target a process group. Never use it for Stop.
    pid > 0 && sender(pid, SIGINT) == 0
}

struct StatusLines {
    private var pending = Data()
    private(set) var rejected = false
    var hasPartialLine: Bool { !pending.isEmpty }
    mutating func feed(_ data: Data) throws -> [Data] {
        if rejected { return [] } // Caller continues draining the pipe until EOF.
        pending.append(data)
        var lines: [Data] = []
        while let newline = pending.firstIndex(of: 10) {
            if pending.distance(from: pending.startIndex, to: newline) > 131072 {
                rejected = true; pending.removeAll(); throw RecorderError.invalidEvent
            }
            lines.append(Data(pending[..<newline])); pending.removeSubrange(...newline)
        }
        if pending.count > 131072 {
            rejected = true; pending.removeAll(); throw RecorderError.invalidEvent
        }
        return lines
    }
}

struct HelperLiveness {
    let command: String
    let started: TimeInterval
    var lastProgress: TimeInterval
    var expired = false
    mutating func check(now: TimeInterval) -> String? {
        if expired { return nil }
        let limit: TimeInterval = command == "access" ? 120 : command == "record" ? 15 : 35
        let reference = ["record", "upload"].contains(command) ? lastProgress : started
        if now - reference <= limit { return nil }
        expired = true
        return command == "record" ? "stop_recording" : command == "upload" ? "cancel_upload" : command == "access" ? "cancel_access" : "terminate"
    }
}

struct SavedRecording: Identifiable, Equatable {
    var id: String { name }
    let name: String
    let path: String
    let closed: Bool
    let uploadable: Bool
    let endReason: String
}

struct RecorderState {
    var phase = "Ready"
    var error: String?
    var confirmedRate: Int?
    var gain: Int?
    var serial = "—"
    var contact = false
    var bytes = 0
    var groups = 0
    var missing = 0
    var decodeErrors = 0
    var constantGroups = 0
    var elapsed = 0.0
    var acknowledged = 0
    var batches = 0
    var installation: String?
    var volumes: [String] = []
    var accessVolume: String?
    var sessions: [SavedRecording] = []
    var currentPath: String?
    var currentUploadable = false
    var terminalReceived = false
    var isRecording = false

    mutating func begin(_ command: String) {
        terminalReceived = false
        error = nil
        isRecording = false
        phase = command == "record" ? "Configuring recorder…" : command == "upload" ? "Preparing upload…" : command == "enroll" ? "Connecting Carolina…" : "Refreshing…"
        if command == "access" { phase = "Allow removable-volume access in the macOS prompt…" }
        if ["access", "state"].contains(command) { accessVolume = nil }
        if command == "record" {
            confirmedRate = nil; gain = nil; bytes = 0; groups = 0; missing = 0
            decodeErrors = 0; constantGroups = 0; elapsed = 0; currentPath = nil
            currentUploadable = false
            serial = "—"; contact = false
        }
        if command == "upload" { acknowledged = 0; batches = 0 }
    }

    mutating func apply(_ row: [String: Any]) throws {
        guard row["schema_version"] as? Int == 1, let event = row["event"] as? String else {
            throw RecorderError.invalidEvent
        }
        switch event {
        case "access_waiting": phase = "Waiting for macOS USB access · accept the removable-volume prompt"
        case "access_granted":
            guard let volume = row["volume"] as? String, volume.hasPrefix("/Volumes/") else { throw RecorderError.invalidEvent }
            accessVolume = volume; phase = "USB access ready · choose settings and Start"
            terminalReceived = true
        case "state":
            volumes = row["volumes"] as? [String] ?? []
            installation = row["installation_id"] as? String
            sessions = (row["sessions"] as? [[String: Any]] ?? []).compactMap { item in
                guard let name = item["name"] as? String, let path = item["path"] as? String,
                      let closed = item["closed"] as? Bool, let reason = item["end_reason"] as? String else { return nil }
                return SavedRecording(name: name, path: path, closed: closed, uploadable: item["uploadable"] as? Bool ?? false, endReason: reason)
            }
            phase = "Ready"; terminalReceived = true
        case "configuring": phase = "Configuring recorder…"
        case "session":
            currentPath = row["path"] as? String
            serial = row["serial"] as? String ?? "—"
        case "confirmed":
            guard let rate = row["sample_rate_hz"] as? Int, [1000,2000,4000,8000,16000,32000].contains(rate),
                  row["gain"] as? Int == 6, let enabled = row["contact_detection"] as? Bool else { throw RecorderError.invalidEvent }
            confirmedRate = rate; gain = 6; contact = enabled
        case "recording":
            guard confirmedRate != nil else { throw RecorderError.unconfirmedRecording }
            phase = "Recording · saved on this Mac"; isRecording = true
        case "progress":
            if terminalReceived { return }
            bytes = row["received_bytes"] as? Int ?? bytes
            groups = row["groups"] as? Int ?? groups
            missing = row["missing_groups"] as? Int ?? missing
            decodeErrors = row["decode_or_sequence_errors"] as? Int ?? decodeErrors
            constantGroups = row["constant_groups"] as? Int ?? constantGroups
            elapsed = row["elapsed_s"] as? Double ?? elapsed
            acknowledged = row["acknowledged"] as? Int ?? acknowledged
            batches = row["batches"] as? Int ?? batches
        case "closing":
            bytes = row["received_bytes"] as? Int ?? bytes
            groups = row["groups"] as? Int ?? groups
            missing = row["missing_groups"] as? Int ?? missing
            decodeErrors = row["decode_or_sequence_errors"] as? Int ?? decodeErrors
            constantGroups = row["constant_groups"] as? Int ?? constantGroups
            elapsed = row["elapsed_s"] as? Double ?? elapsed
            phase = "Stopping and closing local files…"; isRecording = false
        case "saved":
            guard let path = row["path"] as? String,
                  (row["success"] as? Bool == true || row["user_stopped"] as? Bool == true),
                  (row["errors"] as? [String])?.isEmpty == true else { throw RecorderError.invalidEvent }
            currentPath = path; currentUploadable = row["uploadable"] as? Bool ?? false
            phase = currentUploadable ? "Saved on this Mac · ready to upload" : "Stopped before configuration confirmation · raw files retained"
            terminalReceived = true; isRecording = false
        case "preparing": phase = "Preparing immutable upload files…"
        case "uploading": phase = "Uploading to Carolina…"
        case "acknowledged":
            guard let count = row["batches_acknowledged"] as? Int, count > 0 else { throw RecorderError.invalidEvent }
            acknowledged = count; batches = count; phase = "Carolina acknowledged all batches · local files retained"
            terminalReceived = true
        case "enrolled":
            guard let identity = row["installation_id"] as? String, UUID(uuidString: identity) != nil else { throw RecorderError.invalidEvent }
            installation = identity; phase = "Mac connected to Carolina"; terminalReceived = true
        case "error":
            error = row["message"] as? String ?? "Helper failed; local files retained"
            phase = "Needs attention"; terminalReceived = true; isRecording = false
        default: throw RecorderError.invalidEvent
        }
    }

    mutating func finished(exitCode: Int32) {
        isRecording = false
        if !terminalReceived || exitCode != 0 {
            error = error ?? "Helper stopped without confirmed completion (exit \(exitCode)). Local files are retained; device state may be uncertain."
            phase = "Needs attention"
        }
    }
}

enum RecorderError: LocalizedError {
    case invalidEvent, unconfirmedRecording, missingRuntime
    var errorDescription: String? {
        switch self {
        case .invalidEvent: return "Invalid recorder status; local files retained."
        case .unconfirmedRecording: return "Recorder reported acquisition before configuration confirmation."
        case .missingRuntime: return "Python helper/runtime missing. Rebuild the developer app with scripts/build_seca_macos_app.sh."
        }
    }
}
