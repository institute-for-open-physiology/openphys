import Foundation

public enum Ids {
    public static func new() -> String { UUID().uuidString.lowercased() }
}

public enum Clock {
    public static func unixNs(_ date: Date = Date()) -> Int64 {
        Int64((date.timeIntervalSince1970 * 1_000_000).rounded()) * 1_000
    }

    /// Monotonic time that keeps counting while the phone sleeps.
    public static func monotonicNs() -> UInt64 {
        #if canImport(Darwin)
        return clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW)
        #else
        return DispatchTime.now().uptimeNanoseconds
        #endif
    }
}

/// One collector run. Upload batches never mix sessions or writer epochs.
public struct CaptureContext {
    public let sessionId: String
    public let epochId: String
    public var physicalDeviceId: String?
    public let epochStartMonotonicNs: UInt64

    public init(sessionId: String = Ids.new(), epochId: String = Ids.new(), physicalDeviceId: String?, epochStartMonotonicNs: UInt64 = Clock.monotonicNs()) {
        self.sessionId = sessionId
        self.epochId = epochId
        self.physicalDeviceId = physicalDeviceId
        self.epochStartMonotonicNs = epochStartMonotonicNs
    }
}

public struct RawRecord {
    public let recordId: String
    public let connectionId: String?
    public let collectorUnixNs: Int64
    public let collectorMonotonicNs: UInt64
    public let physicalDeviceId: String?
    public let blePeripheralId: String?
    public let kind: String
    public let body: JSONValue

    public init(recordId: String = Ids.new(), connectionId: String?, collectorUnixNs: Int64, collectorMonotonicNs: UInt64,
                physicalDeviceId: String?, blePeripheralId: String?, kind: String, body: JSONValue) {
        self.recordId = recordId
        self.connectionId = connectionId
        self.collectorUnixNs = collectorUnixNs
        self.collectorMonotonicNs = collectorMonotonicNs
        self.physicalDeviceId = physicalDeviceId
        self.blePeripheralId = blePeripheralId
        self.kind = kind
        self.body = body
    }
}

/// Matches the server's IngestRecord shape (src/cloud/ingest.rs).
public enum RecordJSON {
    /// Preserve malformed sensor bytes without presenting them as valid PMD data to the server.
    static func archiveInvalidPMD(_ json: String) -> String? {
        guard var record = try? JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any],
              record["kind"] as? String == "gatt_notification",
              let body = record["body"] as? [String: Any],
              let characteristic = body["characteristic_uuid"] as? String,
              characteristic.lowercased() == H10Protocol.pmdData,
              let payload = body["payload_b64"] as? String, let bytes = Data(base64Encoded: payload),
              PMD.frame(Array(bytes)) == nil else { return nil }
        record["kind"] = "collector_event"
        record["body"] = ["event": "invalid_pmd_packet", "severity": "warn",
                          "detail": "Sensor packet has an invalid header or timestamp.",
                          "fields": ["original_record_json": json]]
        guard let data = try? JSONSerialization.data(withJSONObject: record, options: [.sortedKeys, .withoutEscapingSlashes]) else { return nil }
        return String(decoding: data, as: UTF8.self)
    }

    public static func encode(sourceSeq: Int64, record: RawRecord) -> String {
        JSONValue.object([
            ("record_id", .string(record.recordId)),
            ("source_seq", .int(sourceSeq)),
            ("connection_id", .str(record.connectionId)),
            ("collector_unix_ns", .int(record.collectorUnixNs)),
            ("collector_monotonic_ns", .num(record.collectorMonotonicNs)),
            ("physical_device_id", .str(record.physicalDeviceId)),
            ("ble_peripheral_id", .str(record.blePeripheralId)),
            ("kind", .string(record.kind)),
            ("body", record.body),
        ]).encoded
    }
}
