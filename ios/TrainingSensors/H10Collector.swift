import CoreBluetooth
import Foundation
import H10Kit
import UIKit

/// Polar H10 collector. Mirrors the Android collector's record stream so the server and dashboard need no changes.
struct LogLine: Identifiable {
    let id = UUID()
    let time: Date
    let text: String
}

@MainActor
final class H10Collector: NSObject, ObservableObject {
    @Published private(set) var running = false
    @Published private(set) var state = "Stopped"
    @Published private(set) var detail = ""
    @Published private(set) var bluetoothState: CBManagerState = .unknown
    @Published private(set) var serial = UserDefaults.standard.string(forKey: "h10Serial")
    @Published private(set) var heartRate: Int?
    @Published private(set) var rrMs: Double?
    @Published private(set) var contact: Bool?
    @Published private(set) var battery: Int?
    @Published private(set) var rssi: Int?
    @Published private(set) var streams = "–"
    @Published private(set) var log: [LogLine] = []
    @Published private(set) var connectedAt: Date?
    @Published private(set) var clockSynced = false
    @Published private(set) var marks: [SensorMark] = []
    @Published private(set) var markError: String?
    private var markArchive = MarkArchive()
    private var marksLoaded = false
    let signals = LiveSignals()
    var onHeartRate: ((H10Protocol.HeartRateSample, Date, String?) -> Void)?

    static let model: String = {
        var info = utsname()
        uname(&info)
        return withUnsafeBytes(of: &info.machine) { String(decoding: $0.prefix { $0 != 0 }, as: UTF8.self) }
    }()

    private static let scanTimeout = 10.0
    private static let connectTimeout = 8.0
    nonisolated private static let operationTimeout = 3.0
    // The first encrypted write can wait on the iOS pairing prompt.
    private static let pairingTimeout = 60.0
    private static let watchdog = 2.0
    private static let watchdogGrace = 3.0

    private var stores: [RecordStore] = []
    private var sessionRecord: RawRecord?
    private var connectionRecords: [RawRecord] = []

    func setStores(_ newStores: [RecordStore]) {
        if let context {
            for store in newStores where !stores.contains(where: { $0 === store }) {
                for record in [sessionRecord].compactMap({ $0 }) + connectionRecords {
                    store.append(context: context, record: record)
                }
            }
        }
        stores = newStores
    }
    private var central: CBCentralManager!
    private var context: CaptureContext?
    private var connectionId: String?
    private var peripheralId: String?
    private var peripheral: CBPeripheral?
    private var restoredPeripheral: CBPeripheral?
    private var runTask: Task<Void, Never>?
    private var bluetoothWait: CheckedContinuation<Void, Never>?

    private var pending: Operation?
    private var waiter: Waiter?
    private var responses: [(uuid: String, bytes: [UInt8])] = []
    private var scanWait: CheckedContinuation<CBPeripheral, Error>?
    private var scanAttempt: String?
    private var connectWait: CheckedContinuation<Void, Error>?
    private var connectAttempt: String?
    private var discoveryWait: CheckedContinuation<Void, Error>?
    private var discoveryAttempt: String?
    private var servicesLeft = 0
    private var disconnectReason: String?
    private(set) var ecgAt: Date?
    private var accAt: Date?
    private(set) var hrAt: Date?
    private var hrSample: H10Protocol.HeartRateSample?
    private var tzOffset: Int?

    override init() {
        super.init()
        central = CBCentralManager(delegate: self, queue: nil, options: [CBCentralManagerOptionRestoreIdentifierKey: "training-sensors-h10"])
        UIDevice.current.isBatteryMonitoringEnabled = true
        do { markArchive = try MarkStorage.load(); marks = markArchive.marks; marksLoaded = true }
        catch { markError = "Could not open saved events." }
    }

    func resume() {
        if UserDefaults.standard.object(forKey: "collectionEnabled") == nil || UserDefaults.standard.bool(forKey: "collectionEnabled") { start() }
    }

    func start() {
        guard !running else { return }
        running = true
        UserDefaults.standard.set(true, forKey: "collectionEnabled")
        let previous = runTask
        runTask = Task {
            await previous?.value
            guard !Task.isCancelled else { return }
            await run()
        }
    }

    func stop() {
        guard running else { return }
        running = false
        UserDefaults.standard.set(false, forKey: "collectionEnabled")
        runTask?.cancel()
        bluetoothWait?.resume(); bluetoothWait = nil
        failAll(CancellationError())
        central.stopScan()
        if let peripheral { central.cancelPeripheralConnection(peripheral) }
        if let restoredPeripheral { central.cancelPeripheralConnection(restoredPeripheral); self.restoredPeripheral = nil }
    }

    func forgetSensor() {
        serial = nil
        UserDefaults.standard.removeObject(forKey: "h10Serial")
        UserDefaults.standard.removeObject(forKey: "h10Peripheral")
    }

    // MARK: Run loop

    private func run() async {
        context = CaptureContext(physicalDeviceId: serial.map(H10Identity.physicalDeviceId))
        let info = Bundle.main.infoDictionary
        append("session_started", [
            "software_name": "Training Sensors iOS",
            "software_version": .str(info?["CFBundleShortVersionString"] as? String),
            "git_commit": nil,
            "hostname": .string(Self.model),
            "pid": .num(ProcessInfo.processInfo.processIdentifier),
            "target": .string(serial ?? "any"),
            "configured_h10_serial": .str(serial),
            "physical_device_id": .str(context?.physicalDeviceId),
            "started_reason": "automatic",
            "config_json": "{\"ecg_hz\":130,\"acc_hz\":200,\"watchdog_ms\":2000}",
        ])
        while !Task.isCancelled {
            guard central.state == .poweredOn else {
                switch central.state {
                case .unauthorized: show("Bluetooth access denied", "Allow Bluetooth for Training Sensors in Settings.")
                case .poweredOff: show("Bluetooth off", "Turn on Bluetooth to record.")
                case .unsupported: show("Bluetooth unavailable", "This device does not support Bluetooth LE.")
                default: show("Waiting for Bluetooth", "")
                }
                // Resumed by a Bluetooth state change or stop(), without polling while unavailable.
                await withCheckedContinuation { bluetoothWait = $0 }
                continue
            }
            do {
                let device = try await scan()
                connectionId = Ids.new()
                try await connect(device)
                try await discover(device)
                try await initialize(device)
                try await stream(device)
            } catch {
                if Task.isCancelled { break }
                let message = Self.text(error)
                event("connection_cycle_failed", "error", message, ["connection_id": .str(connectionId)])
                if message.hasPrefix("no matching Polar H10") {
                    show("Searching", "No H10 nearby. Wet the strap pads, wear it, and check the battery.")
                } else {
                    show("Reconnecting", message)
                }
            }
            await cleanup(reason: Task.isCancelled ? "user_stopped" : "cleanup_after_connection_cycle")
            await sleep(0.2 + Double.random(in: 0...0.3))
        }
        await cleanup(reason: "user_stopped")
        append("session_ended", ["reason": "user_disabled", "message": "iOS collector stopped"])
        sessionRecord = nil
        connectionRecords.removeAll()
        context = nil
        show("Stopped", "")
        streams = "–"
        heartRate = nil
    }

    private func scan() async throws -> CBPeripheral {
        try Task.checkCancellation()
        let attempt = Ids.new()
        scanAttempt = attempt
        append("scan_attempt", [
            "attempt_id": .string(attempt), "target": .string(serial ?? "any"),
            "scan_timeout_ms": .num(Int(Self.scanTimeout * 1000)), "service_uuids": [.string(H10Protocol.heartRateService)],
        ])
        if state != "Searching" { show("Scanning", serial.map { "Looking for Polar H10 \($0)" } ?? "Looking for any Polar H10") }
        if let restoredPeripheral,
           recordScanResult(restoredPeripheral, name: restoredPeripheral.name, rssi: nil, advertisement: [:]) {
            return restoredPeripheral
        }
        // An H10 already connected to iOS does not advertise.
        for device in central.retrieveConnectedPeripherals(withServices: [CBUUID(string: "180D")]) {
            if recordScanResult(device, name: device.name, rssi: nil, advertisement: [:]) { return device }
        }
        do {
            let device = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<CBPeripheral, Error>) in
                scanWait = continuation
                central.scanForPeripherals(withServices: [CBUUID(string: "180D")])
                after(Self.scanTimeout) { [weak self] in
                    guard let self, let wait = self.scanWait, self.scanAttempt == attempt else { return }
                    self.scanWait = nil
                    wait.resume(throwing: H10Error("no matching Polar H10 found within 10 s"))
                }
            }
            central.stopScan()
            return device
        } catch {
            central.stopScan()
            append("scan_error", ["attempt_id": .string(attempt), "stage": "ios_ble_scan", "error": .string(Self.text(error))])
            throw error
        }
    }

    /// Records a scan result and reports whether it is the wanted H10.
    private func recordScanResult(_ device: CBPeripheral, name: String?, rssi: Int?, advertisement: [String: Any]) -> Bool {
        let matched = H10Identity.matches(name: name, configuredSerial: serial, peripheralId: device.identifier.uuidString,
                                         savedPeripheralId: UserDefaults.standard.string(forKey: "h10Peripheral"))
        let services = (advertisement[CBAdvertisementDataServiceUUIDsKey] as? [CBUUID] ?? []).map { JSONValue.string(H10Protocol.fullUUID($0.uuidString)) }
        let manufacturer = advertisement[CBAdvertisementDataManufacturerDataKey] as? Data
        append("scan_result", [
            "attempt_id": .str(scanAttempt), "peripheral_id": .string(device.identifier.uuidString.lowercased()),
            "local_name": .str(name), "rssi_dbm": .optNum(rssi),
            "manufacturer_data_b64": .str(manufacturer?.base64EncodedString()),
            "service_uuids": .array(services), "matched": .bool(matched), "configured_h10_serial": .str(serial),
        ])
        return matched
    }

    private func connect(_ device: CBPeripheral) async throws {
        try Task.checkCancellation()
        connectionRecords.removeAll()
        let attempt = Ids.new()
        connectAttempt = attempt
        let started = Date()
        let restored = restoredPeripheral === device
        restoredPeripheral = nil
        peripheral = device
        peripheralId = device.identifier.uuidString.lowercased()
        device.delegate = self
        disconnectReason = nil
        tzOffset = nil
        responses.removeAll()
        append("connect_attempt", ["attempt_id": .string(attempt), "peripheral_id": .str(peripheralId), "local_name": .str(device.name), "rssi_dbm": nil])
        show("Connecting", device.name ?? "Polar H10")
        do {
            // A restored connection belongs to this app. Other system connections still need connect().
            if !restored || device.state != .connected {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    connectWait = continuation
                    if !restored || device.state != .connecting { central.connect(device) }
                    after(Self.connectTimeout) { [weak self] in
                        guard let self, let wait = self.connectWait, self.connectAttempt == attempt else { return }
                        self.connectWait = nil
                        wait.resume(throwing: H10Error("connect timed out"))
                    }
                }
            }
            append("connect_result", ["attempt_id": .string(attempt), "connected": true, "elapsed_ms": .num(Self.ms(since: started))])
        } catch {
            append("connect_error", ["attempt_id": .string(attempt), "stage": "connect", "error": .string(Self.text(error)), "elapsed_ms": .num(Self.ms(since: started))])
            throw error
        }
    }

    private func discover(_ device: CBPeripheral) async throws {
        try Task.checkCancellation()
        let attempt = Ids.new()
        discoveryAttempt = attempt
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            discoveryWait = continuation
            device.discoverServices(nil)
            after(8) { [weak self] in
                guard let self, let wait = self.discoveryWait, self.discoveryAttempt == attempt else { return }
                self.discoveryWait = nil
                wait.resume(throwing: H10Error("service discovery timed out"))
            }
        }
    }

    private func initialize(_ device: CBPeripheral) async throws {
        var info: [String: String?] = [:]
        let fields: [(String, String, String)] = [
            (H10Protocol.manufacturerName, "manufacturer", "manufacturer_name"), (H10Protocol.modelNumber, "model", "model_number"),
            (H10Protocol.serialNumber, "serial", "serial_number"), (H10Protocol.hardwareRevision, "hardware_revision", "hardware_revision"),
            (H10Protocol.firmwareRevision, "firmware_revision", "firmware_revision"), (H10Protocol.softwareRevision, "software_revision", "software_revision"),
        ]
        for (uuid, label, reason) in fields {
            guard let characteristic = characteristic(device, uuid) else { info[label] = .some(nil); continue }
            let bytes = try? await read(device, characteristic, reason: reason)
            info[label] = .some(bytes.map { String(decoding: $0, as: UTF8.self).trimmingCharacters(in: CharacterSet(charactersIn: "\u{0}")) })
        }
        let advertised = H10Identity.serial(fromAdvertisedName: device.name)
        guard let observed = H10Identity.normalizedSerial(info["serial"] ?? nil), serial == nil || observed == serial,
              advertised == nil || advertised == observed else {
            let message = "H10 serial mismatch: configured=\(serial ?? "any") observed=\((info["serial"] ?? nil) ?? "none")"
            event("h10_identity_mismatch", "critical", message, ["configured_h10_serial": .str(serial), "observed_h10_serial": .str(info["serial"] ?? nil)])
            throw H10Error(message)
        }
        if serial == nil {
            serial = observed
            UserDefaults.standard.set(observed, forKey: "h10Serial")
        }
        UserDefaults.standard.set(device.identifier.uuidString, forKey: "h10Peripheral")
        context?.physicalDeviceId = H10Identity.physicalDeviceId(serial: observed)
        event("h10_identity_verified", "info", "Device Information serial matches configured H10", [
            "configured_h10_serial": .string(observed), "observed_h10_serial": .string(observed),
            "physical_device_id": .str(context?.physicalDeviceId), "ble_peripheral_id": .str(peripheralId),
        ])
        try? await readBattery(device)
        append("service_discovery_summary", summary(device, info: info))
        guard let hr = characteristic(device, H10Protocol.heartRateMeasurement),
              let control = characteristic(device, H10Protocol.pmdControl),
              let data = characteristic(device, H10Protocol.pmdData) else { throw H10Error("required H10 characteristic missing") }
        _ = try? await read(device, control, reason: "pmd_features")
        try await subscribe(device, hr, reason: "heart_rate_measurement")
        try await subscribe(device, control, reason: "pmd_control")
        try await subscribe(device, data, reason: "pmd_data")
        for (command, reason) in [(H10Protocol.ecgStop, "pre_start_stop_ecg"), (H10Protocol.accStop, "pre_start_stop_acc")] {
            do {
                _ = try await write(device, control, command, reason: reason, timeout: Self.pairingTimeout)
                _ = try await awaitNotification(H10Protocol.pmdControl, timeout: Self.operationTimeout) { H10Protocol.isMatchingControlResponse($0, command: command) }
            } catch {
                event("pmd_pre_start_stop_failed", "warn", Self.text(error), ["reason": .string(reason)])
            }
        }
        await setLocalTime(device)
        await sleep(0.1)
        try await startStream(device, control, H10Protocol.ecgStart, reason: "pmd_start_ecg")
        await sleep(0.1)
        try await startStream(device, control, H10Protocol.accStart, reason: "pmd_start_acc")
        event("pmd_stream_start_confirmed", "info", "ecg+acc", ["ecg_hz": 130, "acc_hz": 200])
        ecgAt = nil; accAt = nil; hrAt = nil
        connectedAt = Date()
        show("Streaming", "ECG + motion + heart rate")
    }

    private func startStream(_ device: CBPeripheral, _ control: CBCharacteristic, _ command: [UInt8], reason: String) async throws {
        _ = try await write(device, control, command, reason: reason, timeout: Self.pairingTimeout)
        let response = try await awaitNotification(H10Protocol.pmdControl, timeout: Self.operationTimeout) { H10Protocol.isMatchingControlResponse($0, command: command) }
        guard H10Protocol.controlSucceeded(response) else { throw H10Error("\(reason) rejected: \(Hashing.hex(response))") }
    }

    /// Sets the H10 clock so PMD device timestamps line up with phone time. Failure is recorded and retried next connection.
    private func setLocalTime(_ device: CBPeripheral) async {
        guard let mtu = characteristic(device, H10Protocol.psftpMtu) else {
            event("psftp_set_local_time_unavailable", "warn", "PS-FTP characteristic absent")
            return
        }
        let operation = Ids.new()
        let started = Date()
        do {
            try await subscribe(device, mtu, reason: "psftp_mtu", timeout: Self.pairingTimeout)
            let time = H10Protocol.psftpLocalTime()
            append("psftp_set_local_time_attempt", [
                "operation_id": .string(operation), "local_time_iso8601": .string(time.iso8601),
                "local_time_unix_ns": .int(time.unixNs), "tz_offset_minutes": .num(time.offsetMinutes), "dst_offset_minutes": nil,
                "request_payload_b64": .string(Data(time.parameters).base64EncodedString()),
                "control_characteristic_uuid": .string(H10Protocol.psftpMtu), "mtu_characteristic_uuid": .string(H10Protocol.psftpMtu),
            ])
            for packet in time.packets { _ = try await write(device, mtu, packet, reason: "psftp_set_local_time_packet", timeout: Self.pairingTimeout) }
            var assembler = H10Protocol.PsftpResponseAssembler()
            var response: [UInt8]?
            while response == nil {
                response = try assembler.accept(try await awaitNotification(H10Protocol.psftpMtu, timeout: 8) { _ in true })
            }
            append("psftp_set_local_time_result", psftpResult(operation, success: true, response: response ?? [], error: nil, started: started))
            tzOffset = time.offsetMinutes
            clockSynced = true
            note("Clock synced")
        } catch {
            append("psftp_set_local_time_result", psftpResult(operation, success: false, response: [], error: Self.text(error), started: started))
            event("psftp_set_local_time_failed", "warn", Self.text(error))
        }
    }

    private func psftpResult(_ id: String, success: Bool, response: [UInt8], error: String?, started: Date) -> JSONValue {
        ["operation_id": .string(id), "success": .bool(success), "status_code": success ? .int(0) : .null,
         "response_payload_b64": .string(Data(response).base64EncodedString()), "error": .str(error), "elapsed_ms": .num(Self.ms(since: started))]
    }

    private func stream(_ device: CBPeripheral) async throws {
        let started = Date()
        var lastBattery = Date()
        var lastHrWarning = Date.distantPast
        while !Task.isCancelled {
            if let reason = disconnectReason { throw H10Error(reason) }
            if let value = try? await readRSSI(device) {
                rssi = value
                signals.addRSSI(value)
            }
            let backlogs = stores.map { $0.backlog() }
            let backlog = Backlog(records: backlogs.map(\.records).max() ?? 0, bytes: backlogs.map(\.bytes).max() ?? 0)
            let level = UIDevice.current.batteryLevel
            event("collector_health", "info", "streaming", [
                "backlog_records": .num(backlog.records), "backlog_bytes": .num(backlog.bytes),
                "phone_battery_percent": level < 0 ? .null : .num(Int(level * 100)),
                "power_save_mode": .bool(ProcessInfo.processInfo.isLowPowerModeEnabled),
                "device_interactive": .bool(UIApplication.shared.applicationState == .active),
                "battery_charging": .bool(UIDevice.current.batteryState == .charging || UIDevice.current.batteryState == .full),
                "ecg_age_ms": Self.age(ecgAt), "acc_age_ms": Self.age(accAt), "hr_age_ms": Self.age(hrAt),
            ])
            for store in stores { if let failure = store.lastFailure { event("local_queue_failure", "critical", failure) } }
            if Date().timeIntervalSince(lastBattery) >= 60 { try? await readBattery(device); lastBattery = Date() }
            let warm = Date().timeIntervalSince(started) > Self.watchdogGrace
            if warm && Self.stale(ecgAt) {
                event("ecg_watchdog_timeout", "error", "ECG stale >2s")
                throw H10Error("ECG stream watchdog timeout")
            }
            if warm && Self.stale(accAt) {
                event("acc_watchdog_timeout", "error", "ACC stale >2s")
                throw H10Error("ACC stream watchdog timeout")
            }
            if Self.stale(hrAt) && Date().timeIntervalSince(lastHrWarning) > Self.watchdog {
                lastHrWarning = Date()
                event("hr_watchdog_timeout", "warn", "HR stale >2s")
            }
            refreshHeartRate()
            streams = [("ECG", ecgAt), ("ACC", accAt), ("HR", hrAt)].map { $0.0 + (Self.stale($0.1) ? " ✗" : " ✓") }.joined(separator: "  ")
            await sleep(1)
        }
    }

    private func cleanup(reason: String) async {
        connectedAt = nil
        clockSynced = false
        heartRate = nil
        rrMs = nil
        contact = nil
        hrSample = nil
        hrAt = nil
        battery = nil
        signals.history.flush()
        guard let device = peripheral else { return }
        let started = Date()
        append("disconnect_attempt", ["reason": .string(reason), "peripheral_id": .str(peripheralId)])
        if device.state != .disconnected {
            central.cancelPeripheralConnection(device)
            for _ in 0..<20 where device.state != .disconnected { await sleep(0.1) }
        }
        if device.state == .disconnected {
            append("disconnect_result", ["reason": .string(reason), "elapsed_ms": .num(Self.ms(since: started))])
        } else {
            append("disconnect_error", ["reason": .string(reason), "error": "disconnect timed out", "elapsed_ms": .num(Self.ms(since: started))])
        }
        peripheral = nil
        peripheralId = nil
        connectionId = nil
        rssi = nil
    }

    // MARK: GATT operations

    private final class Operation {
        let id = Ids.new()
        let kind: String
        let uuid: String
        let reason: String
        let started = Date()
        var continuation: CheckedContinuation<[UInt8], Error>?
        init(_ kind: String, _ uuid: String, _ reason: String) { self.kind = kind; self.uuid = uuid; self.reason = reason }
    }

    private struct Waiter {
        let uuid: String
        let matches: ([UInt8]) -> Bool
        let continuation: CheckedContinuation<[UInt8], Error>
        let id: String
    }

    private func perform(_ operation: Operation, timeout: Double, _ start: () -> Void) async throws -> [UInt8] {
        try Task.checkCancellation()
        return try await withCheckedThrowingContinuation { continuation in
            pending?.continuation?.resume(throwing: H10Error("superseded by \(operation.kind)"))
            operation.continuation = continuation
            pending = operation
            start()
            after(timeout) { [weak self] in self?.finish(operation, .failure(H10Error("\(operation.kind) timed out"))) }
        }
    }

    private func finish(_ operation: Operation, _ result: Result<[UInt8], Error>) {
        guard let continuation = operation.continuation else { return }
        operation.continuation = nil
        if pending === operation { pending = nil }
        continuation.resume(with: result)
    }

    private func read(_ device: CBPeripheral, _ characteristic: CBCharacteristic, reason: String) async throws -> [UInt8] {
        let operation = Operation("read", uuid(characteristic), reason)
        append("gatt_read_request", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid), "reason": .string(reason)])
        do {
            return try await perform(operation, timeout: Self.operationTimeout) { device.readValue(for: characteristic) }
        } catch {
            append("gatt_read_error", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid), "reason": .string(reason),
                                       "error": .string(Self.text(error)), "elapsed_ms": .num(Self.ms(since: operation.started))])
            throw error
        }
    }

    private func write(_ device: CBPeripheral, _ characteristic: CBCharacteristic, _ payload: [UInt8], reason: String, timeout: Double = H10Collector.operationTimeout) async throws -> [UInt8] {
        let operation = Operation("write", uuid(characteristic), reason)
        append("gatt_write_request", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid), "write_type": "with_response",
                                      "reason": .string(reason), "payload_b64": .string(Data(payload).base64EncodedString()), "payload_len": .num(payload.count)])
        do {
            return try await perform(operation, timeout: timeout) { device.writeValue(Data(payload), for: characteristic, type: .withResponse) }
        } catch {
            append("gatt_write_error", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid),
                                        "error": .string(Self.text(error)), "elapsed_ms": .num(Self.ms(since: operation.started))])
            throw error
        }
    }

    private func subscribe(_ device: CBPeripheral, _ characteristic: CBCharacteristic, reason: String, timeout: Double = H10Collector.operationTimeout) async throws {
        let operation = Operation("subscription", uuid(characteristic), reason)
        let indicate = characteristic.properties.contains(.indicate) && !characteristic.properties.contains(.notify)
        append("subscription_attempt", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid),
                                         "notify_kind": indicate ? "indicate" : "notify", "reason": .string(reason)])
        do {
            _ = try await perform(operation, timeout: timeout) {
                if characteristic.isNotifying {
                    complete("subscription", "subscription_result", uuid(characteristic), nil, Clock.unixNs(), Clock.monotonicNs())
                } else { device.setNotifyValue(true, for: characteristic) }
            }
        } catch {
            append("subscription_error", ["operation_id": .string(operation.id), "characteristic_uuid": .string(operation.uuid),
                                          "error": .string(Self.text(error)), "elapsed_ms": .num(Self.ms(since: operation.started))])
            throw error
        }
    }

    private func readRSSI(_ device: CBPeripheral) async throws -> Int {
        let operation = Operation("rssi", "", "ios_read_rssi")
        do {
            let bytes = try await perform(operation, timeout: 0.5) { device.readRSSI() }
            return Int(Int8(bitPattern: bytes.first ?? 0))
        } catch {
            append("rssi_read_error", ["peripheral_id": .str(peripheralId), "method": "ios_read_rssi", "error": .string(Self.text(error)),
                                       "elapsed_ms": .num(Self.ms(since: operation.started))])
            throw error
        }
    }

    private func readBattery(_ device: CBPeripheral) async throws {
        guard let characteristic = characteristic(device, H10Protocol.batteryLevel) else { return }
        if let level = try await read(device, characteristic, reason: "battery_level").first { battery = Int(level) }
    }

    private func awaitNotification(_ uuid: String, timeout: Double, matching: @escaping ([UInt8]) -> Bool) async throws -> [UInt8] {
        try Task.checkCancellation()
        if let index = responses.firstIndex(where: { $0.uuid == uuid && matching($0.bytes) }) {
            return responses.remove(at: index).bytes
        }
        let id = Ids.new()
        return try await withCheckedThrowingContinuation { continuation in
            waiter = Waiter(uuid: uuid, matches: matching, continuation: continuation, id: id)
            after(timeout) { [weak self] in
                guard let self, let waiter = self.waiter, waiter.id == id else { return }
                self.waiter = nil
                waiter.continuation.resume(throwing: H10Error("no \(H10Protocol.describe(uuid)) response within \(Int(timeout)) s"))
            }
        }
    }

    private func failAll(_ error: Error) {
        if let wait = scanWait { scanWait = nil; wait.resume(throwing: error) }
        if let operation = pending { finish(operation, .failure(error)) }
        if let waiter { self.waiter = nil; waiter.continuation.resume(throwing: error) }
        if let wait = connectWait { connectWait = nil; wait.resume(throwing: error) }
        if let wait = discoveryWait { discoveryWait = nil; wait.resume(throwing: error) }
    }

    private func handleNotification(_ uuid: String, _ bytes: [UInt8], unixNs: Int64, monoNs: UInt64) {
        append("gatt_notification", ["characteristic_uuid": .string(uuid), "description": .string(H10Protocol.describe(uuid)),
                                      "payload_b64": .string(Data(bytes).base64EncodedString()), "payload_len": .num(bytes.count)],
               unixNs: unixNs, monoNs: monoNs)
        switch uuid {
        case H10Protocol.pmdData:
            guard PMD.frame(bytes) != nil else { return }
            if bytes.first == 0 { ecgAt = Date() } else if bytes.first == 2 { accAt = Date() }
            signals.addPMD(bytes, unixNs: unixNs, offsetMinutes: tzOffset)
        case H10Protocol.heartRateMeasurement:
            if let sample = H10Protocol.parseHeartRate(bytes) {
                hrAt = Date()
                hrSample = sample
                if sample.hasHeartRate { signals.addHeartRate(sample.bpm, rr: sample.rrMs) }
                onHeartRate?(sample, Date(timeIntervalSince1970: Double(unixNs) / 1_000_000_000), serial)
                let detected = sample.contact(after: 0)
                if let previous = contact, let detected, previous != detected { note(detected ? "Skin contact restored" : "Skin contact lost") }
                refreshHeartRate()
            }
        case H10Protocol.pmdControl, H10Protocol.psftpMtu:
            if let waiter, waiter.uuid == uuid, waiter.matches(bytes) {
                self.waiter = nil
                waiter.continuation.resume(returning: bytes)
            } else {
                responses.append((uuid, bytes))
                if responses.count > 32 { responses.removeFirst() }
            }
        default: break
        }
    }

    // MARK: Records

    private func refreshHeartRate() {
        let age = hrAt.map { Date().timeIntervalSince($0) } ?? .infinity
        contact = hrSample?.contact(after: age)
        let valid = age < H10Protocol.HeartRateSample.maximumAge && hrSample?.hasHeartRate == true
        heartRate = valid ? hrSample?.bpm : nil
        rrMs = valid ? hrSample?.rrMs.last : nil
    }

    private func append(_ kind: String, _ body: JSONValue, unixNs: Int64? = nil, monoNs: UInt64? = nil) {
        guard let context else { return }
        let mono = monoNs ?? Clock.monotonicNs()
        let record = RawRecord(
            connectionId: connectionId, collectorUnixNs: unixNs ?? Clock.unixNs(),
            collectorMonotonicNs: mono >= context.epochStartMonotonicNs ? mono - context.epochStartMonotonicNs : 0,
            physicalDeviceId: context.physicalDeviceId, blePeripheralId: peripheralId, kind: kind, body: body)
        if kind == "session_started" { sessionRecord = record; connectionRecords.removeAll() }
        if ["connect_attempt", "connect_result", "service_discovery_summary", "psftp_set_local_time_attempt", "psftp_set_local_time_result"].contains(kind) {
            connectionRecords.append(record)
        }
        for store in stores { store.append(context: context, record: record) }
    }

    func addMark() {
        guard state == "Streaming" else { return }
        let mark = SensorMark(id: Ids.new(), time: Date())
        var archive = markArchive
        archive.marks = marks + [mark]
        guard saveMarks(archive) else { return }
        event("manual_marker", "info", mark.label, ["id": .string(mark.id), "label": .string(mark.label),
              "marked_unix_ms": .num(Int64(mark.time.timeIntervalSince1970 * 1000))])
        UIImpactFeedbackGenerator(style: .light).impactOccurred()
    }

    func receiveWatchMark(_ mark: SensorMark) -> Bool {
        guard marksLoaded, UUID(uuidString: mark.id) != nil, mark.time.timeIntervalSince1970.isFinite, mark.time.timeIntervalSince1970 >= 0,
              mark.time <= Date().addingTimeInterval(60) else { return false }
        var archive = markArchive
        let added = archive.receive(SensorMark(id: mark.id, time: mark.time))
        if added {
            if context == nil { context = CaptureContext(physicalDeviceId: serial.map(H10Identity.physicalDeviceId)) }
            event("manual_marker", "info", "Watch event", ["id": .string(mark.id), "label": "Event", "source": "watch",
                  "marked_unix_ms": .num(Int64(mark.time.timeIntervalSince1970 * 1000))])
            do { for store in stores { try store.flush() } }
            catch { markError = "Could not queue this event. The Watch will retry."; return false }
        }
        return saveMarks(archive)
    }

    private func saveMarks(_ archive: MarkArchive) -> Bool {
        guard marksLoaded else { return false }
        do {
            #if DEBUG
            if !ProcessInfo.processInfo.arguments.contains("-demo") { try archive.write(to: MarkStorage.url) }
            #else
            try archive.write(to: MarkStorage.url)
            #endif
            markArchive = archive; marks = archive.marks; markError = nil
            return true
        } catch { markError = "Could not save this event. Try again."; return false }
    }

    func renameMark(_ id: String, to label: String) {
        guard let index = marks.firstIndex(where: { $0.id == id }) else { return }
        let name = String(label.trimmingCharacters(in: .whitespacesAndNewlines).prefix(80))
        var archive = markArchive
        archive.marks = marks
        archive.marks[index].label = name.isEmpty ? "Event" : name
        guard saveMarks(archive) else { return }
        if context == nil { context = CaptureContext(physicalDeviceId: serial.map(H10Identity.physicalDeviceId)) }
        event("manual_marker_updated", "info", marks[index].label, ["id": .string(id), "label": .string(marks[index].label),
              "marked_unix_ms": .num(Int64(marks[index].time.timeIntervalSince1970 * 1000))])
    }

    func deleteMark(_ id: String) {
        guard let mark = marks.first(where: { $0.id == id }) else { return }
        var archive = markArchive
        archive.marks = marks.filter { $0.id != id }
        guard saveMarks(archive) else { return }
        if context == nil { context = CaptureContext(physicalDeviceId: serial.map(H10Identity.physicalDeviceId)) }
        event("manual_marker_deleted", "info", mark.label, ["id": .string(id), "label": .string(mark.label),
              "marked_unix_ms": .num(Int64(mark.time.timeIntervalSince1970 * 1000))])
    }

    private func note(_ text: String) {
        log.append(LogLine(time: Date(), text: text))
        if log.count > 50 { log.removeFirst(log.count - 50) }
    }

    private func event(_ name: String, _ severity: String, _ detail: String, _ fields: KeyValuePairs<String, JSONValue> = [:]) {
        if severity != "info" { note(name.replacingOccurrences(of: "_", with: " ").capitalized + ": " + detail) }
        append("collector_event", ["event": .string(name), "severity": .string(severity), "detail": .string(detail), "fields": .object(fields.map { ($0.key, $0.value) })])
    }

    private func summary(_ device: CBPeripheral, info: [String: String?]) -> JSONValue {
        let services = (device.services ?? []).sorted { uuid($0.uuid) < uuid($1.uuid) }.map { service -> JSONValue in
            let characteristics = (service.characteristics ?? []).sorted { uuid($0) < uuid($1) }.map { characteristic -> JSONValue in
                let p = characteristic.properties
                let pairs: [(CBCharacteristicProperties, String)] = [(.read, "read"), (.write, "write"), (.writeWithoutResponse, "write_without_response"),
                                                                   (.notify, "notify"), (.indicate, "indicate")]
                let names = pairs.filter { p.contains($0.0) }.map { JSONValue.string($0.1) }
                return ["characteristic_uuid": .string(uuid(characteristic)), "properties": .array(names)]
            }
            return ["service_uuid": .string(uuid(service.uuid)), "characteristics": .array(characteristics)]
        }
        let keys = ["manufacturer", "model", "serial", "hardware_revision", "firmware_revision", "software_revision"]
        return ["peripheral_id": .str(peripheralId), "service_count": .num(services.count), "services": .array(services),
                "device_info": .object(keys.map { key -> (String, JSONValue) in (key, JSONValue.str(info[key] ?? nil)) })]
    }

    // MARK: Helpers

    private func characteristic(_ device: CBPeripheral, _ wanted: String) -> CBCharacteristic? {
        (device.services ?? []).lazy.flatMap { $0.characteristics ?? [] }.first { uuid($0) == wanted }
    }

    private func uuid(_ characteristic: CBCharacteristic) -> String { uuid(characteristic.uuid) }
    private func uuid(_ value: CBUUID) -> String { H10Protocol.fullUUID(value.uuidString) }

    private func show(_ state: String, _ detail: String) {
        if state != self.state || detail != self.detail { note(detail.isEmpty ? state : state + ": " + detail) }
        self.state = state
        self.detail = detail
    }

    private func after(_ seconds: Double, _ work: @escaping @MainActor () -> Void) {
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            work()
        }
    }

    private func sleep(_ seconds: Double) async {
        try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
    }

    private static func stale(_ date: Date?) -> Bool { date.map { Date().timeIntervalSince($0) > watchdog } ?? true }
    private static func age(_ date: Date?) -> JSONValue { date.map { JSONValue.num(Int(Date().timeIntervalSince($0) * 1000)) } ?? .null }
    private static func ms(since date: Date) -> Int { max(0, Int(Date().timeIntervalSince(date) * 1000)) }
    private static func text(_ error: Error) -> String {
        String(((error as? H10Error)?.message ?? error.localizedDescription).prefix(240))
    }
}

// MARK: CoreBluetooth delegates (delivered on the main queue)

extension H10Collector: CBCentralManagerDelegate, CBPeripheralDelegate {
    nonisolated func centralManagerDidUpdateState(_ central: CBCentralManager) {
        MainActor.assumeIsolated {
            if !ProcessInfo.processInfo.arguments.contains("-demo") { bluetoothState = central.state }
            bluetoothWait?.resume(); bluetoothWait = nil
            if central.state != .poweredOn {
                disconnectReason = "Bluetooth is not available"
                failAll(H10Error("Bluetooth is not available"))
            }
        }
    }

    nonisolated func centralManager(_ central: CBCentralManager, willRestoreState dict: [String: Any]) {
        MainActor.assumeIsolated {
            let devices = dict[CBCentralManagerRestoredStatePeripheralsKey] as? [CBPeripheral] ?? []
            let enabled = UserDefaults.standard.object(forKey: "collectionEnabled") == nil || UserDefaults.standard.bool(forKey: "collectionEnabled")
            for device in devices {
                if enabled, restoredPeripheral == nil,
                   H10Identity.matches(name: device.name, configuredSerial: serial, peripheralId: device.identifier.uuidString,
                                       savedPeripheralId: UserDefaults.standard.string(forKey: "h10Peripheral")) {
                    restoredPeripheral = device
                    device.delegate = self
                } else { central.cancelPeripheralConnection(device) }
            }
        }
    }

    nonisolated func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral,
                                    advertisementData: [String: Any], rssi RSSI: NSNumber) {
        MainActor.assumeIsolated {
            guard scanWait != nil else { return }
            let name = advertisementData[CBAdvertisementDataLocalNameKey] as? String ?? peripheral.name
            if recordScanResult(peripheral, name: name, rssi: RSSI.intValue, advertisement: advertisementData), let wait = scanWait {
                scanWait = nil
                wait.resume(returning: peripheral)
            }
        }
    }

    nonisolated func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId else { return }
            centralEvent("device_connected", "ios_did_connect", unixNs: unixNs, monoNs: monoNs)
            if let wait = connectWait { connectWait = nil; wait.resume() }
        }
    }

    nonisolated func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId else { return }
            if let wait = connectWait { connectWait = nil; wait.resume(throwing: error ?? H10Error("connect failed")) }
        }
    }

    nonisolated func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId else { return }
            let reason = "GATT disconnected: " + (error.map { Self.text($0) } ?? "no error")
            centralEvent("device_disconnected", reason, unixNs: unixNs, monoNs: monoNs)
            disconnectReason = reason
            failAll(H10Error(reason))
        }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didDiscoverServices error: Error?) {
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId, let wait = discoveryWait else { return }
            let services = peripheral.services ?? []
            if let error { discoveryWait = nil; wait.resume(throwing: error); return }
            servicesLeft = services.count
            if services.isEmpty { discoveryWait = nil; wait.resume(throwing: H10Error("H10 exposed no services")) }
            for service in services { peripheral.discoverCharacteristics(nil, for: service) }
        }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didDiscoverCharacteristicsFor service: CBService, error: Error?) {
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId, let wait = discoveryWait else { return }
            if let error { discoveryWait = nil; wait.resume(throwing: error); return }
            servicesLeft -= 1
            if servicesLeft <= 0 { discoveryWait = nil; wait.resume() }
        }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didUpdateValueFor characteristic: CBCharacteristic, error: Error?) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        let bytes = [UInt8](characteristic.value ?? Data())
        MainActor.assumeIsolated {
            guard peripheral.identifier.uuidString.lowercased() == peripheralId else { return }
            let id = uuid(characteristic)
            // CoreBluetooth reports reads and notifications through the same callback.
            if let operation = pending, operation.kind == "read", operation.uuid == id {
                if let error { finish(operation, .failure(error)); return }
                append("gatt_read_response", ["operation_id": .string(operation.id), "characteristic_uuid": .string(id), "reason": .string(operation.reason),
                                              "payload_b64": .string(Data(bytes).base64EncodedString()), "payload_len": .num(bytes.count)],
                       unixNs: unixNs, monoNs: monoNs)
                finish(operation, .success(bytes))
            } else if error == nil {
                handleNotification(id, bytes, unixNs: unixNs, monoNs: monoNs)
            }
        }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didWriteValueFor characteristic: CBCharacteristic, error: Error?) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        MainActor.assumeIsolated { complete("write", "gatt_write_result", uuid(characteristic), error, unixNs, monoNs) }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didUpdateNotificationStateFor characteristic: CBCharacteristic, error: Error?) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        MainActor.assumeIsolated { complete("subscription", "subscription_result", uuid(characteristic), error, unixNs, monoNs) }
    }

    nonisolated func peripheral(_ peripheral: CBPeripheral, didReadRSSI RSSI: NSNumber, error: Error?) {
        let unixNs = Clock.unixNs(), monoNs = Clock.monotonicNs()
        MainActor.assumeIsolated {
            guard let operation = pending, operation.kind == "rssi" else { return }
            if let error { finish(operation, .failure(error)); return }
            append("rssi_sample", ["peripheral_id": .str(peripheralId), "rssi_dbm": .num(RSSI.intValue), "method": "ios_read_rssi", "age_ms": 0],
                   unixNs: unixNs, monoNs: monoNs)
            finish(operation, .success([UInt8(bitPattern: Int8(clamping: RSSI.intValue))]))
        }
    }

    private func complete(_ kind: String, _ resultKind: String, _ id: String, _ error: Error?, _ unixNs: Int64, _ monoNs: UInt64) {
        guard let operation = pending, operation.kind == kind, operation.uuid == id else { return }
        if let error { finish(operation, .failure(error)); return }
        append(resultKind, ["operation_id": .string(operation.id), "characteristic_uuid": .string(id), "elapsed_ms": .num(Self.ms(since: operation.started))],
               unixNs: unixNs, monoNs: monoNs)
        finish(operation, .success([]))
    }

    private func centralEvent(_ name: String, _ detail: String, unixNs: Int64, monoNs: UInt64) {
        append("central_event", ["event_name": .string(name), "peripheral_id": .str(peripheralId), "detail": .string(detail),
                                 "rssi_dbm": nil, "service_uuids": [], "raw_debug": "CBCentralManagerDelegate"],
               unixNs: unixNs, monoNs: monoNs)
    }
}


#if DEBUG
import QuartzCore

extension H10Collector {
    private func startSnapshot() {
        let fixture = DashboardFixture.load()
        signals.history.persistent = false
        running = true
        state = "Streaming"
        detail = "ECG + motion + heart rate"
        battery = fixture.battery
        rssi = fixture.rssi
        contact = true
        heartRate = fixture.bpm
        rrMs = fixture.rr
        clockSynced = true
        connectedAt = Date(timeIntervalSince1970: fixture.time - 1471)
        marks = fixture.events.map { SensorMark(id: $0.id, time: Date(timeIntervalSince1970: $0.time), label: $0.label) }
        log = [LogLine(time: connectedAt!, text: "Streaming: ECG + motion + heart rate")]
        let offset = DisplayClock.uptime - fixture.time
        for row in fixture.history {

            signals.rr.append(row[0] + offset, [row[2]])
            signals.rssi.append(row[0] + offset, [row[4]])
            signals.delay.append(row[0] + offset, [row[5], row[5]])
        }
        for row in fixture.heartRate { signals.heartRate.append(row[0] + offset, [row[1]]) }
        signals.addECG(fixture.ecg.map { Int32($0[1]) }, DisplayClock.uptime)
        signals.addACC(Array(repeating: fixture.acceleration.map { Int32($0) }, count: 200), DisplayClock.uptime)
        signals.addHeartRate(fixture.bpm, rr: [fixture.rr])
        signals.history.replace(with: fixture.history.map { (time: $0[0], values: $0.dropFirst().map(Float.init)) })
    }

    /// Simulator preview: synthetic streams so the screens can be reviewed without a strap.
    func startDemo() {
        bluetoothState = .poweredOn
        if ProcessInfo.processInfo.arguments.contains("-bluetooth-off") { bluetoothState = .poweredOff; return }
        if ProcessInfo.processInfo.arguments.contains("-bluetooth-denied") { bluetoothState = .unauthorized; return }
        if DisplayClock.snapshot { startSnapshot(); return }
        signals.history.persistent = false
        marks = [SensorMark(id: "demo", time: Date().addingTimeInterval(-240), label: "Walk")]
        connectedAt = Date().addingTimeInterval(-1471)
        clockSynced = true
        let end = Date().timeIntervalSince1970
        signals.history.replace(with: (0..<720).map { i in
            let t = Double(i)
            return (time: end - 3600 + t * 5, values: [Float(72 + 9 * sin(t / 23)), Float(833 + 80 * sin(t / 19)), Float(20 + 80 * pow(sin(t / 32), 8)), Float(-58 + 8 * sin(t / 17)), Float(36 + 5 * sin(t / 5))])
        })
        if ProcessInfo.processInfo.arguments.contains("-disconnected") {
            connectedAt = nil
            clockSynced = false
            return
        }
        running = true
        serial = "C7A1B2C3"
        battery = 84
        contact = true
        show("Streaming", "ECG + motion + heart rate")
        let origin = CACurrentMediaTime()
        for i in 0..<300 {
            let s = Double(i)
            signals.heartRate.append(origin - 600 + s * 2, [72 + 9 * sin(s / 23) + 4 * sin(s / 7)])
            signals.rssi.append(origin - 600 + s * 2, [-60 + 7 * sin(s / 17)])
            signals.addDelay(36 + 5 * sin(s / 5) + Double.random(in: -4...4), origin - 300 + s)
        }
        Task { @MainActor in
            var beats = [0.3]
            var ecgCount = 0, accCount = 0, second = 0
            while running {
                let t = CACurrentMediaTime() - origin
                while beats.last! < t + 1 { beats.append(beats.last! + 60 / (68 + 7 * sin(beats.last! / 9))) }
                if beats.count > 20 { beats.removeFirst(beats.count - 20) }
                var ecg: [Int32] = []
                while Double(ecgCount) / 130 < t {
                    let s = Double(ecgCount) / 130
                    var value = 60 * sin(2 * .pi * 0.25 * s)
                    for beat in beats where abs(s - beat) < 0.6 {
                        let d = s - beat
                        value += 1100 * exp(-d * d / 0.000242) + 280 * exp(-pow(d - 0.28, 2) / 0.00405) + 120 * exp(-pow(d + 0.16, 2) / 0.00125)
                    }
                    ecg.append(Int32(value))
                    ecgCount += 1
                }
                if !ecg.isEmpty { signals.addECG(ecg, CACurrentMediaTime()) }
                var acc: [[Int32]] = []
                while Double(accCount) / 200 < t {
                    let s = Double(accCount) / 200
                    acc.append([Int32(-960 + 40 * sin(s * 1.3)), Int32(90 + 25 * sin(s * 2.1)), Int32(245 + 30 * sin(s * 0.7))])
                    accCount += 1
                }
                if !acc.isEmpty { signals.addACC(acc, CACurrentMediaTime()) }
                if Int(t) > second {
                    second = Int(t)
                    let past = beats.filter { $0 < t }
                    let rr = past.count >= 2 ? (past[past.count - 1] - past[past.count - 2]) * 1000 : 880
                    if !ProcessInfo.processInfo.arguments.contains("-stale-hr") || second < 3 {
                        let interval = Int(rr * 1024 / 1000)
                        let payload: [UInt8] = ProcessInfo.processInfo.arguments.contains("-contact-lost") ? [0x04, 0]
                            : [0x16, UInt8((60_000 / rr).rounded()), UInt8(interval & 255), UInt8(interval >> 8)]
                        handleNotification(H10Protocol.heartRateMeasurement, payload, unixNs: Clock.unixNs(), monoNs: Clock.monotonicNs())
                    }
                    refreshHeartRate()
                    rssi = -58 + Int(6 * sin(t / 7))
                    signals.addRSSI(rssi!)
                    signals.addDelay(36 + 5 * sin(t) + Double.random(in: -4...4), CACurrentMediaTime())
                }
                await sleep(0.1)
            }
        }
    }
}
#endif
