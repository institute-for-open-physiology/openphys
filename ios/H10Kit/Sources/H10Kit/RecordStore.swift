import Foundation

public struct UploadBatch: Codable, Equatable, Sendable {
    public let batchId: String
    public let installationId: String
    public let captureSessionId: String
    public let writerEpochId: String
    public let firstSourceSeq: Int64
    public let lastSourceSeq: Int64
    public let recordCount: Int
    public let body: String
    public let bodySha256: String

    public var encodedBytes: Int { body.utf8.count }
}

public struct Backlog: Equatable, Sendable {
    public let records: Int
    public let bytes: Int

    public init(records: Int, bytes: Int) {
        self.records = records
        self.bytes = bytes
    }
}

/// Durable upload queue. Only a batch and small read/write buffers are held in memory.
/// Records and exact retry bytes stay on disk until the server durably acknowledges them.
public final class RecordStore: @unchecked Sendable {
    public static let maxRecords = 256
    public static let maxBatchBytes = 512 * 1024

    private struct Pending {
        let session: String
        let epoch: String
        let seq: Int64
        let json: String
    }

    private final class Reader {
        let handle: FileHandle
        private var buffer = Data()
        private(set) var offset: UInt64
        private(set) var groupOffset: UInt64
        private let decoder = QueueCompression.Decoder()

        init(_ url: URL, offset: UInt64 = 0, groupOffset: UInt64 = 0) throws {
            handle = try FileHandle(forReadingFrom: url)
            self.offset = groupOffset
            self.groupOffset = groupOffset
            try handle.seek(toOffset: groupOffset)
            while self.offset < offset {
                guard try next() != nil else { throw H10Error("queue position is missing") }
            }
            guard self.offset == offset else { throw H10Error("queue position is not a record boundary") }
        }

        deinit { try? handle.close() }

        func next() throws -> Pending? {
            #if canImport(ObjectiveC)
            return try autoreleasepool { try readNext() }
            #else
            return try readNext()
            #endif
        }

        private func readNext() throws -> Pending? {
            while true {
                if buffer.first == 72 {
                    guard try fill(QueueCompression.headerBytes) else { return nil }
                    let header = try QueueCompression.readHeader(buffer)
                    let length = QueueCompression.headerBytes + header.compressedBytes
                    guard try fill(length) else { return nil }
                    let data = try decoder.decode(Data(buffer.dropFirst(QueueCompression.headerBytes).prefix(header.compressedBytes)), header: header)
                    guard data.last == 10, let line = String(data: data.dropLast(), encoding: .utf8),
                          let entry = RecordStore.parse(line[...]) else { throw H10Error("compressed queue contains an unreadable record") }
                    if header.reset { groupOffset = offset }
                    buffer.removeFirst(length)
                    offset += UInt64(length)
                    return entry
                }
                guard buffer.prefix(while: { $0 != 9 && $0 != 10 }).allSatisfy({ (32..<127).contains($0) }) else {
                    throw H10Error("queue record header is unreadable")
                }
                if let newline = buffer.firstIndex(of: 10) {
                    let length = buffer.distance(from: buffer.startIndex, to: newline) + 1
                    guard let line = String(data: buffer.prefix(length - 1), encoding: .utf8),
                          let entry = RecordStore.parse(line[...]) else { throw H10Error("queue contains an unreadable record") }
                    buffer.removeFirst(length)
                    offset += UInt64(length)
                    groupOffset = offset
                    return entry
                }
                guard buffer.count <= RecordStore.maxBatchBytes * 2 else { throw H10Error("queue record exceeds read limit") }
                // A crash can leave a partial final line. offset stays at the last complete record.
                if try !fill(buffer.count + 1) { return nil }
            }
        }

        private func fill(_ count: Int) throws -> Bool {
            while buffer.count < count {
                guard let data = try handle.read(upToCount: max(64 * 1024, count - buffer.count)), !data.isEmpty else { return false }
                // Copy only the unread tail so Data slices cannot retain a growing consumed prefix.
                var next = Data(capacity: buffer.count + data.count)
                next.append(buffer)
                next.append(data)
                buffer = next
            }
            return true
        }
    }

    private let io = DispatchQueue(label: "training-sensors.record-store", autoreleaseFrequency: .workItem)
    private let writes = DispatchSemaphore(value: 16)
    private let directory: URL
    private let installationId: String
    private let appendBytes: (FileHandle, Data) throws -> Void
    private var pendingCount = 0
    private var pendingBytes = 0
    private var headOffset: UInt64 = 0
    private var headGroupOffset: UInt64 = 0
    private var endOffset: UInt64 = 0
    private var nextSeq: Int64 = 0
    private var lastAck: Int64 = -1
    private var appendHandle: FileHandle?
    private var failure: String?
    private var unsavedRecords = 0
    private var failedRecord: (context: CaptureContext, record: RawRecord)?
    private let encoder = QueueCompression.Encoder()

    private var pendingURL: URL { directory.appendingPathComponent("pending.tsv") }
    private var batchURL: URL { directory.appendingPathComponent("batch.json") }
    private var ackURL: URL { directory.appendingPathComponent("last_ack") }

    public convenience init(directory: URL, installationId: String) throws {
        try self.init(directory: directory, installationId: installationId, appendBytes: { try $0.write(contentsOf: $1) })
    }

    init(directory: URL, installationId: String, appendBytes: @escaping (FileHandle, Data) throws -> Void) throws {
        self.directory = directory
        self.installationId = installationId
        self.appendBytes = appendBytes
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: ackURL.path) {
            let data = try readSmall(ackURL, limit: 128)
            guard let value = Int64(String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)), value >= -1 else {
                throw H10Error("queue acknowledgement is unreadable")
            }
            lastAck = value
        }
        nextSeq = lastAck + 1
        if FileManager.default.fileExists(atPath: pendingURL.path) {
            let reader = try Reader(pendingURL)
            func readEntry() throws -> Bool {
                guard let entry = try reader.next() else { return false }
                if entry.seq <= lastAck { headOffset = reader.offset; headGroupOffset = reader.groupOffset; return true }
                guard entry.seq == nextSeq else { throw H10Error("queue sequence gap at \(nextSeq)") }
                pendingCount += 1
                pendingBytes += entry.json.utf8.count
                nextSeq += 1
                return true
            }
            while true {
                #if canImport(ObjectiveC)
                let found = try autoreleasepool(invoking: readEntry)
                #else
                let found = try readEntry()
                #endif
                if !found { break }
            }
            endOffset = reader.offset
        } else { try Data().write(to: pendingURL, options: .atomic) }
        let handle = try FileHandle(forWritingTo: pendingURL)
        // Remove only an incomplete tail, never a complete unacknowledged record.
        try handle.truncate(atOffset: endOffset)
        try handle.seekToEnd()
        appendHandle = handle
    }

    deinit { try? appendHandle?.close() }

    /// Bound queued writes too, so a slow disk cannot accumulate unlimited closures and records.
    public func append(context: CaptureContext, record: RawRecord) {
        writes.wait()
        io.async {
            defer { self.writes.signal() }
            self.appendLocked(context: context, record: record)
        }
    }

    public func flush() throws {
        try io.sync {
            if let failure { throw H10Error(failure) }
            guard let appendHandle else { throw H10Error("queue file is not open") }
            try appendHandle.synchronize()
        }
    }

    public func backlog() -> Backlog { io.sync { Backlog(records: pendingCount, bytes: pendingBytes) } }
    public var lastAcknowledged: Int64 { io.sync { lastAck } }
    public var lastFailure: String? { io.sync { failure } }

    /// Retry the interrupted record and durably log any readings lost while storage was unavailable.
    @discardableResult
    public func resumeWrites() throws -> Int {
        try io.sync {
            guard failure != nil else { return 0 }
            encoder.reset()
            let handle = try FileHandle(forWritingTo: pendingURL)
            guard try handle.seekToEnd() >= endOffset else { throw H10Error("queue file was truncated") }
            try handle.truncate(atOffset: endOffset)
            try handle.seekToEnd()
            if let failedRecord {
                let mono = Clock.monotonicNs()
                let start = failedRecord.context.epochStartMonotonicNs
                let recovered = RawRecord(connectionId: failedRecord.record.connectionId, collectorUnixNs: Clock.unixNs(),
                                          collectorMonotonicNs: mono >= start ? mono - start : 0, physicalDeviceId: failedRecord.record.physicalDeviceId,
                                          blePeripheralId: failedRecord.record.blePeripheralId, kind: "collector_event",
                                          body: ["event": "local_queue_recovered", "severity": unsavedRecords > 0 ? "error" : "info",
                                                 "detail": "Storage writes resumed", "fields": ["unsaved_records": .num(unsavedRecords)]])
                let entries = [failedRecord.record, recovered].enumerated().map { index, record in
                    Pending(session: failedRecord.context.sessionId, epoch: failedRecord.context.epochId,
                            seq: nextSeq + Int64(index), json: RecordJSON.encode(sourceSeq: nextSeq + Int64(index), record: record))
                }
                var data = Data()
                for entry in entries { data.append(try encoder.encode(Data(Self.line(entry).utf8))) }
                try appendBytes(handle, data)
                try handle.synchronize()
                endOffset += UInt64(data.count)
                pendingCount += entries.count
                pendingBytes += entries.reduce(0) { $0 + $1.json.utf8.count }
                nextSeq += Int64(entries.count)
                self.failedRecord = nil
            } else { try handle.synchronize() }
            try? appendHandle?.close()
            appendHandle = handle
            failure = nil
            let lost = unsavedRecords
            unsavedRecords = 0
            return lost
        }
    }

    public func nextBatch() throws -> UploadBatch? { try io.sync { try nextBatchLocked() } }

    /// Call only after HTTP 400. Ambiguous failures must retain identical retry bytes.
    public func recoverRejectedBatch(_ batch: UploadBatch) throws -> Bool {
        try io.sync {
            let data = try readSmall(batchURL, limit: Self.maxBatchBytes * 4)
            let current = try JSONDecoder().decode(UploadBatch.self, from: data)
            guard current == batch, batch.lastSourceSeq > lastAck else { return false }
            let reader = try Reader(pendingURL, offset: headOffset, groupOffset: headGroupOffset)
            var original: [Pending] = []
            var bytes = 0
            while original.count < min(batch.recordCount, Self.maxRecords), let entry = try reader.next() {
                bytes += entry.json.utf8.count
                guard bytes <= Self.maxBatchBytes else { throw H10Error("rejected batch exceeds read limit") }
                original.append(entry)
            }
            guard original.count == batch.recordCount, original.first?.seq == batch.firstSourceSeq,
                  original.last?.seq == batch.lastSourceSeq,
                  encodeBatch(original, id: batch.batchId) == batch.body,
                  original.contains(where: { RecordJSON.archiveInvalidPMD($0.json) != nil }) else { return false }
            let rejected = directory.appendingPathComponent("rejected", isDirectory: true)
            try FileManager.default.createDirectory(at: rejected, withIntermediateDirectories: true)
            try writeDurably(data, to: rejected.appendingPathComponent(batch.batchId + ".json"))
            _ = try createBatchLocked()
            return true
        }
    }

    public func acknowledge(_ batch: UploadBatch, committedThrough: Int64) throws {
        try io.sync {
            guard committedThrough == batch.lastSourceSeq else { throw H10Error("server ACK did not match uploaded batch") }
            if committedThrough <= lastAck { return }
            guard batch.firstSourceSeq == lastAck + 1, committedThrough < nextSeq else { throw H10Error("server ACK exceeds local sequence") }
            let reader = try Reader(pendingURL, offset: headOffset, groupOffset: headGroupOffset)
            var dropped = 0, bytes = 0
            while dropped < batch.recordCount {
                guard let entry = try reader.next(), entry.seq == lastAck + 1 + Int64(dropped) else { throw H10Error("queue changed before acknowledgement") }
                dropped += 1
                bytes += entry.json.utf8.count
            }
            guard lastAck + Int64(dropped) == committedThrough else { throw H10Error("ACK record count mismatch") }
            try writeDurably(Data(String(committedThrough).utf8), to: ackURL)
            lastAck = committedThrough
            headOffset = reader.offset
            headGroupOffset = reader.groupOffset
            pendingCount -= dropped
            pendingBytes -= bytes
            try? FileManager.default.removeItem(at: batchURL)
            // A failed compaction leaves the original file usable. Retry after another ACK.
            try? compact()
        }
    }

    private func appendLocked(context: CaptureContext, record: RawRecord) {
        guard failure == nil else {
            if failedRecord == nil { failedRecord = (context, record) }
            else { unsavedRecords += 1 }
            return
        }
        let entry = Pending(session: context.sessionId, epoch: context.epochId, seq: nextSeq,
                            json: RecordJSON.encode(sourceSeq: nextSeq, record: record))
        do {
            guard let handle = appendHandle else { throw H10Error("queue file is not open") }
            let data = try encoder.encode(Data(Self.line(entry).utf8))
            try appendBytes(handle, data)
            endOffset += UInt64(data.count)
            pendingCount += 1
            pendingBytes += entry.json.utf8.count
            nextSeq += 1
        } catch { encoder.reset(); failedRecord = (context, record); failure = "queue write failed: \(error)" }
    }

    private func nextBatchLocked() throws -> UploadBatch? {
        if FileManager.default.fileExists(atPath: batchURL.path) {
            let batch = try JSONDecoder().decode(UploadBatch.self, from: readSmall(batchURL, limit: Self.maxBatchBytes * 4))
            if batch.lastSourceSeq > lastAck {
                guard batch.firstSourceSeq == lastAck + 1, batch.lastSourceSeq < nextSeq else { throw H10Error("saved batch does not match the queue") }
                try synchronize(batchURL)
                return batch
            }
            try FileManager.default.removeItem(at: batchURL)
        }
        return try createBatchLocked()
    }

    private func createBatchLocked() throws -> UploadBatch? {
        guard pendingCount > 0 else { return nil }
        try appendHandle?.synchronize()
        let reader = try Reader(pendingURL, offset: headOffset, groupOffset: headGroupOffset)
        var selected: [Pending] = []
        var bytes = 512
        while selected.count < min(Self.maxRecords, pendingCount), let entry = try reader.next() {
            if let first = selected.first, entry.session != first.session || entry.epoch != first.epoch { break }
            guard entry.seq == lastAck + 1 + Int64(selected.count) else { throw H10Error("queue sequence gap") }
            let json = RecordJSON.archiveInvalidPMD(entry.json) ?? entry.json
            let nextBytes = bytes + json.utf8.count + 1
            if !selected.isEmpty && nextBytes > Self.maxBatchBytes { break }
            guard nextBytes <= Self.maxBatchBytes else { throw H10Error("single record exceeds batch size limit") }
            selected.append(Pending(session: entry.session, epoch: entry.epoch, seq: entry.seq, json: json))
            bytes = nextBytes
        }
        guard let first = selected.first, let last = selected.last else { throw H10Error("queued records are missing") }
        let batchId = Ids.new()
        let body = encodeBatch(selected, id: batchId)
        guard body.utf8.count <= Self.maxBatchBytes else { throw H10Error("encoded batch exceeds size limit") }
        let batch = UploadBatch(batchId: batchId, installationId: installationId, captureSessionId: first.session,
                                writerEpochId: first.epoch, firstSourceSeq: first.seq, lastSourceSeq: last.seq,
                                recordCount: selected.count, body: body, bodySha256: Hashing.sha256Hex(Array(body.utf8)))
        try writeDurably(JSONEncoder().encode(batch), to: batchURL)
        return batch
    }

    private func encodeBatch(_ selected: [Pending], id: String) -> String {
        JSONValue.object([
            ("schema_version", 1), ("installation_id", .string(installationId)), ("batch_id", .string(id)),
            ("source", "chest_h10"), ("capture_session_id", .string(selected[0].session)), ("writer_epoch_id", .string(selected[0].epoch)),
            ("first_source_seq", .int(selected[0].seq)), ("last_source_seq", .int(selected[selected.count - 1].seq)),
            ("records", .array(selected.map { .raw($0.json) })),
        ]).encoded
    }

    private func readSmall(_ url: URL, limit: Int) throws -> Data {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        let data = try handle.read(upToCount: limit + 1) ?? Data()
        guard data.count <= limit else { throw H10Error("queue metadata exceeds read limit") }
        return data
    }

    private func writeDurably(_ data: Data, to url: URL) throws {
        try data.write(to: url, options: .atomic)
        try synchronize(url)
    }

    private func synchronize(_ url: URL) throws {
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        try handle.synchronize()
    }

    private func compact() throws {
        let remaining = endOffset - headOffset
        // Keep rewrites short and their extra disk space small while a large backlog drains.
        guard pendingBytes <= 4 * 1024 * 1024, headOffset > max(4 * 1024 * 1024, remaining) || pendingCount == 0 else { return }
        let temporary = directory.appendingPathComponent("pending.compacting")
        try Data().write(to: temporary)
        defer { try? FileManager.default.removeItem(at: temporary) }
        let reader = try Reader(pendingURL, offset: headOffset, groupOffset: headGroupOffset)
        let compressor = QueueCompression.Encoder()
        let writer = try FileHandle(forWritingTo: temporary)
        defer { try? writer.close() }
        var size: UInt64 = 0
        for _ in 0..<pendingCount {
            guard let entry = try reader.next() else { throw H10Error("queue ended during compaction") }
            let data = try compressor.encode(Data(Self.line(entry).utf8))
            try writer.write(contentsOf: data)
            size += UInt64(data.count)
        }
        try writer.synchronize()
        _ = try FileManager.default.replaceItemAt(pendingURL, withItemAt: temporary)
        headOffset = 0
        headGroupOffset = 0
        endOffset = size
        encoder.reset()
        try? appendHandle?.close()
        appendHandle = nil
        do {
            let handle = try FileHandle(forWritingTo: pendingURL)
            try handle.seekToEnd()
            appendHandle = handle
        } catch { failure = "queue reopen failed: \(error)"; throw error }
    }

    private static func line(_ entry: Pending) -> String {
        entry.session + "\t" + entry.epoch + "\t" + String(entry.seq) + "\t" + entry.json + "\n"
    }

    private static func parse(_ line: Substring) -> Pending? {
        let fields = line.split(separator: "\t", maxSplits: 3, omittingEmptySubsequences: false)
        guard fields.count == 4, let seq = Int64(fields[2]), fields[3].hasPrefix("{"), fields[3].hasSuffix("}") else { return nil }
        return Pending(session: String(fields[0]), epoch: String(fields[1]), seq: seq, json: String(fields[3]))
    }
}

/// Validates the server's durable ACK the same way the Android uploader does.
public enum IngestAck {
    public static func committedThrough(responseBody: Data, batch: UploadBatch) throws -> Int64 {
        guard let json = try JSONSerialization.jsonObject(with: responseBody) as? [String: Any] else {
            throw H10Error("ACK is not a JSON object")
        }
        func int(_ key: String) throws -> Int64 {
            guard let value = integer(json[key]) else { throw H10Error("ACK missing \(key)") }
            return value
        }
        func string(_ key: String) throws -> String {
            guard let value = json[key] as? String, !value.isEmpty else { throw H10Error("ACK missing \(key)") }
            return value
        }
        guard try int("schema_version") == 1 else { throw H10Error("ACK schema_version mismatch") }
        guard try string("installation_id").lowercased() == batch.installationId.lowercased() else { throw H10Error("ACK installation mismatch") }
        guard try string("batch_id").lowercased() == batch.batchId.lowercased() else { throw H10Error("ACK batch mismatch") }
        guard try string("batch_checksum_sha256") == batch.bodySha256 else { throw H10Error("ACK checksum mismatch") }
        guard try int("encoded_bytes") == Int64(batch.encodedBytes) else { throw H10Error("ACK encoded_bytes mismatch") }
        guard try int("record_count") == Int64(batch.recordCount) else { throw H10Error("ACK record_count mismatch") }
        let accepted = try int("accepted_records")
        guard accepted == 0 || accepted == Int64(batch.recordCount) else { throw H10Error("ACK accepted_records invalid") }
        let committed = try int("durably_committed_through_seq")
        guard committed <= batch.lastSourceSeq else { throw H10Error("ACK exceeds uploaded batch sequence") }
        return committed
    }

    /// The server's machine-readable error code, e.g. device_reconnect_required.
    public static func machineCode(_ body: Data) -> String? {
        guard let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else { return nil }
        return json["code"] as? String
    }

    static func integer(_ value: Any?) -> Int64? {
        if let v = value as? Int { return Int64(v) }
        if let v = value as? Int64 { return v }
        if let v = value as? NSNumber { return v.int64Value }
        if let v = value as? Double, v == v.rounded() { return Int64(v) }
        return nil
    }
}
