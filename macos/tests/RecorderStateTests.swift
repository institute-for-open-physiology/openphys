import Foundation

@main struct RecorderStateTests {
    static func main() throws {
        var signalled: [(Int32, Int32)] = []
        assert(!interruptSupervisor(pid: 0, sender: { signalled.append(($0, $1)); return 0 }))
        assert(!interruptSupervisor(pid: -123, sender: { signalled.append(($0, $1)); return 0 }))
        assert(signalled.isEmpty)
        assert(interruptSupervisor(pid: 123, sender: { signalled.append(($0, $1)); return 0 }))
        assert(signalled.count == 1 && signalled[0].0 == 123 && signalled[0].1 == SIGINT)
        assert(!interruptSupervisor(pid: 123, sender: { _, _ in -1 }))
        // Real supervisor + child: Stop must reach the positive supervisor PID
        // while leaving the child available for cleanup commands.
        let supervisor = Process(), supervisorOutput = Pipe()
        supervisor.executableURL = URL(fileURLWithPath: "/usr/bin/python3")
        supervisor.arguments = ["-u", "-c", """
import signal, subprocess, sys, time
stopped = False
def stop(*_):
    global stopped
    stopped = True
signal.signal(signal.SIGINT, stop)
worker = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
print('ready', flush=True)
while not stopped:
    time.sleep(.01)
time.sleep(.05)
print('worker_alive=' + str(worker.poll() is None), flush=True)
worker.terminate()
worker.wait(timeout=3)
"""]
        supervisor.standardOutput = supervisorOutput
        try supervisor.run()
        let ready = supervisorOutput.fileHandleForReading.availableData
        assert(String(data: ready, encoding: .utf8)?.contains("ready") == true)
        assert(interruptSupervisor(pid: supervisor.processIdentifier))
        let completion = supervisorOutput.fileHandleForReading.readDataToEndOfFile()
        supervisor.waitUntilExit()
        assert(supervisor.terminationStatus == 0)
        assert(String(data: completion, encoding: .utf8)?.contains("worker_alive=True") == true)
        var lines = StatusLines()
        let partial = try lines.feed(Data("partial".utf8))
        assert(partial.isEmpty)
        assert(lines.hasPartialLine)
        let split = try lines.feed(Data(" line\nnext\n".utf8))
        assert(split.count == 2 && !lines.hasPartialLine)
        var longState = StatusLines()
        let bigValid = try longState.feed(Data(repeating: 32, count: 100000) + Data([10]))
        assert(bigValid.count == 1)
        var rejected = StatusLines()
        do { _ = try rejected.feed(Data(repeating: 65, count: 131073)); fatalError("oversized status") } catch {}
        let discarded = try rejected.feed(Data("ignored\n".utf8))
        assert(rejected.rejected && discarded.isEmpty)
        var rejectedLine = StatusLines()
        do { _ = try rejectedLine.feed(Data(repeating: 65, count: 131073) + Data([10])); fatalError("oversized line") } catch {}
        // A real oversized producer must exit even after framing rejects its
        // output: consume/discard all bytes rather than abandoning a full pipe.
        let child = Process(), output = Pipe()
        child.executableURL = URL(fileURLWithPath: "/usr/bin/python3")
        child.arguments = ["-c", "import sys; sys.stdout.buffer.write(b'x'*262144+b'\\n'); sys.stdout.flush()"]
        child.standardOutput = output
        try child.run()
        var framing = StatusLines(), drained = 0, invalid = false
        while true {
            let bytes = output.fileHandleForReading.availableData
            if bytes.isEmpty { break }
            drained += bytes.count
            do { _ = try framing.feed(bytes) } catch { invalid = true }
        }
        child.waitUntilExit()
        assert(invalid && drained == 262145 && child.terminationStatus == 0)
        var recordWatch = HelperLiveness(command: "record", started: 0, lastProgress: 0)
        assert(recordWatch.check(now: 15) == nil)
        assert(recordWatch.check(now: 16) == "stop_recording")
        assert(recordWatch.check(now: 40) == nil)
        var uploadWatch = HelperLiveness(command: "upload", started: 0, lastProgress: 0)
        for now in stride(from: 20.0, through: 200.0, by: 20) {
            uploadWatch.lastProgress = now // Durable preparation progress, no ACK.
            assert(uploadWatch.check(now: now + 10) == nil)
        }
        assert(uploadWatch.check(now: 236) == "cancel_upload")
        var controlWatch = HelperLiveness(command: "enroll", started: 0, lastProgress: 100)
        assert(controlWatch.check(now: 36) == "terminate")
        var accessWatch = HelperLiveness(command: "access", started: 0, lastProgress: 0)
        assert(accessWatch.check(now: 119) == nil)
        assert(accessWatch.check(now: 121) == "cancel_access")
        var state = RecorderState()
        func event(_ name: String, _ fields: [String: Any] = [:]) -> [String: Any] {
            var row = fields; row["schema_version"] = 1; row["event"] = name; return row
        }
        state.begin("access")
        try state.apply(event("access_waiting"))
        try state.apply(event("access_granted", ["volume": "/Volumes/fake"]))
        assert(state.accessVolume == "/Volumes/fake" && state.terminalReceived)
        state.begin("state")
        assert(state.accessVolume == nil)
        do { try state.apply(event("access_granted", ["volume": "/tmp/other"])); fatalError("invalid access volume") } catch {}
        state.begin("record")
        assert(state.confirmedRate == nil && state.error == nil)
        do { try state.apply(event("recording")); fatalError("unconfirmed run accepted") } catch {}
        do { try state.apply(event("confirmed", ["sample_rate_hz": 1234, "gain": 6, "contact_detection": false])); fatalError("invalid rate") } catch {}
        try state.apply(event("session", ["path": "/private/recording", "serial": "synthetic"]))
        try state.apply(event("confirmed", ["sample_rate_hz": 32000, "gain": 6, "contact_detection": false]))
        try state.apply(event("recording"))
        assert(state.isRecording && state.confirmedRate == 32000 && !state.contact)
        try state.apply(event("progress", ["groups": 10, "missing_groups": 2, "decode_or_sequence_errors": 1, "received_bytes": 4000, "constant_groups": 9, "elapsed_s": 1.5]))
        assert(state.groups == 10 && state.missing == 2 && state.bytes == 4000 && state.elapsed == 1.5)
        try state.apply(event("closing", ["groups": 11]))
        assert(!state.isRecording && state.groups == 11)
        try state.apply(event("saved", ["path": "/private/recording", "success": false, "user_stopped": true, "uploadable": true, "errors": [String]()]))
        state.finished(exitCode: 0)
        assert(state.error == nil && state.terminalReceived && state.currentUploadable)
        try state.apply(event("progress", ["groups": 999]))
        assert(state.groups == 11)
        state.begin("record")
        try state.apply(event("saved", ["path": "/private/unconfirmed", "success": false, "user_stopped": true, "uploadable": false, "errors": [String]()]))
        assert(!state.currentUploadable && state.phase.contains("before configuration"))
        state.begin("upload")
        try state.apply(event("preparing")); try state.apply(event("uploading"))
        try state.apply(event("progress", ["acknowledged": 1, "batches": 2]))
        assert(state.acknowledged == 1 && state.batches == 2)
        try state.apply(event("acknowledged", ["batches_acknowledged": 2]))
        state.finished(exitCode: 0)
        assert(state.error == nil && state.acknowledged == 2)
        state.begin("state")
        try state.apply(event("state", ["volumes": ["/Volumes/test"], "installation_id": "identity", "sessions": [["name": "recording", "path": "/private/recording", "closed": true, "end_reason": "interrupted"], ["invalid": "ignored"]]]))
        assert(state.volumes.count == 1 && state.sessions.count == 1 && state.sessions[0].closed)
        state.begin("enroll")
        try state.apply(event("enrolled", ["installation_id": "10000000-0000-0000-0000-000000000001"]))
        assert(state.installation != nil && state.terminalReceived)
        for row in [["event": "state"], event("unknown"), event("enrolled"), event("acknowledged"), event("saved", ["path": "x", "success": false, "errors": ["disk failed"]])] {
            do { try state.apply(row); fatalError("invalid event accepted") } catch {}
        }
        state.begin("record"); state.finished(exitCode: 0)
        assert(state.error != nil && !state.isRecording)
        state.begin("upload"); try state.apply(event("error", ["message": "HTTP failure, retained"]))
        state.finished(exitCode: 1)
        assert(state.error == "HTTP failure, retained")
        assert(RecorderError.missingRuntime.errorDescription != nil)
        assert(RecorderError.invalidEvent.errorDescription != nil)
        assert(RecorderError.unconfirmedRecording.errorDescription != nil)
        print("RecorderState contract checks passed")
    }
}
