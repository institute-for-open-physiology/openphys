import Foundation
import CZlib

/// H1Z + reset byte + little-endian UInt32 lengths (raw, compressed), CRC32, header CRC32, then zlib data.
/// Each frame flushes one TSV row; dictionaries reset after 256 KiB so seeks need only a short replay.
enum QueueCompression {
    static let headerBytes = 20
    static let groupBytes = 256 * 1024
    static let recordLimit = RecordStore.maxBatchBytes * 2

    struct Header {
        let reset: Bool
        let rawBytes: Int
        let compressedBytes: Int
        let checksum: UInt32
    }

    static func readHeader(_ data: Data) throws -> Header {
        let bytes = Array(data.prefix(headerBytes))
        guard bytes.count == headerBytes, bytes.prefix(3) == [72, 49, 90], bytes[3] <= 1 else {
            throw H10Error("queue compression header is unreadable")
        }
        func word(_ offset: Int) -> UInt32 {
            (0..<4).reduce(0) { $0 | UInt32(bytes[offset + $1]) << ($1 * 8) }
        }
        guard checksum(Data(bytes.prefix(16))) == word(16) else { throw H10Error("queue compression header checksum mismatch") }
        let raw = Int(word(4)), compressed = Int(word(8))
        guard raw > 0, raw <= recordLimit, compressed > 0, compressed <= recordLimit + 1024 else {
            throw H10Error("compressed queue record exceeds read limit")
        }
        return Header(reset: bytes[3] == 1, rawBytes: raw, compressedBytes: compressed, checksum: word(12))
    }

    static func checksum(_ data: Data) -> UInt32 {
        data.withUnsafeBytes { UInt32(crc32(0, $0.bindMemory(to: Bytef.self).baseAddress, uInt(data.count))) }
    }

    final class Encoder {
        private let stream = UnsafeMutablePointer<z_stream>.allocate(capacity: 1)
        private var ready = false
        private var rawBytes = 0

        init() { stream.initialize(to: z_stream()) }
        deinit { reset(); stream.deinitialize(count: 1); stream.deallocate() }

        func reset() {
            if ready { deflateEnd(stream) }
            ready = false
            rawBytes = 0
            stream.pointee = z_stream()
        }

        func encode(_ data: Data) throws -> Data {
            guard !data.isEmpty, data.count <= recordLimit else { throw H10Error("queue record exceeds compression limit") }
            if rawBytes >= groupBytes { reset() }
            let startsGroup = !ready
            if !ready {
                guard deflateInit_(stream, 5, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else {
                    throw H10Error("queue compressor could not start")
                }
                ready = true
            }
            var output = Data(count: Int(deflateBound(stream, uLong(data.count))) + 16)
            let capacity = output.count
            let result = data.withUnsafeBytes { source in
                output.withUnsafeMutableBytes { target in
                    stream.pointee.next_in = UnsafeMutablePointer(mutating: source.bindMemory(to: Bytef.self).baseAddress)
                    stream.pointee.avail_in = uInt(data.count)
                    stream.pointee.next_out = target.bindMemory(to: Bytef.self).baseAddress
                    stream.pointee.avail_out = uInt(capacity)
                    return deflate(stream, Z_SYNC_FLUSH)
                }
            }
            defer { stream.pointee.next_in = nil; stream.pointee.next_out = nil }
            guard result == Z_OK, stream.pointee.avail_in == 0, stream.pointee.avail_out > 0 else {
                reset()
                throw H10Error("queue compression failed")
            }
            output.count = capacity - Int(stream.pointee.avail_out)
            var frame = Data([72, 49, 90, startsGroup ? 1 : 0])
            for value in [UInt32(data.count), UInt32(output.count), checksum(data)] {
                for shift in stride(from: 0, to: 32, by: 8) { frame.append(UInt8(truncatingIfNeeded: value >> shift)) }
            }
            let headerChecksum = checksum(frame)
            for shift in stride(from: 0, to: 32, by: 8) { frame.append(UInt8(truncatingIfNeeded: headerChecksum >> shift)) }
            frame.append(output)
            rawBytes += data.count
            return frame
        }
    }

    final class Decoder {
        private let stream = UnsafeMutablePointer<z_stream>.allocate(capacity: 1)
        private var ready = false
        private var rawBytes = 0

        init() { stream.initialize(to: z_stream()) }
        deinit { if ready { inflateEnd(stream) }; stream.deinitialize(count: 1); stream.deallocate() }

        func decode(_ data: Data, header: Header) throws -> Data {
            if header.reset {
                if ready { inflateEnd(stream); ready = false }
                stream.pointee = z_stream()
                guard inflateInit_(stream, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else {
                    throw H10Error("queue decompressor could not start")
                }
                ready = true
                rawBytes = 0
            }
            guard ready else { throw H10Error("compressed queue group is missing its start") }
            guard rawBytes < groupBytes else { throw H10Error("compressed queue group exceeds replay limit") }
            // One spare byte distinguishes the exact size from truncated output.
            var output = Data(count: header.rawBytes + 1)
            let capacity = output.count
            let result = data.withUnsafeBytes { source in
                output.withUnsafeMutableBytes { target in
                    stream.pointee.next_in = UnsafeMutablePointer(mutating: source.bindMemory(to: Bytef.self).baseAddress)
                    stream.pointee.avail_in = uInt(data.count)
                    stream.pointee.next_out = target.bindMemory(to: Bytef.self).baseAddress
                    stream.pointee.avail_out = uInt(capacity)
                    return inflate(stream, Z_SYNC_FLUSH)
                }
            }
            defer { stream.pointee.next_in = nil; stream.pointee.next_out = nil }
            guard result == Z_OK, stream.pointee.avail_in == 0,
                  capacity - Int(stream.pointee.avail_out) == header.rawBytes else {
                throw H10Error("compressed queue record is unreadable")
            }
            output.count = header.rawBytes
            guard checksum(output) == header.checksum else { throw H10Error("compressed queue checksum mismatch") }
            rawBytes += output.count
            return output
        }
    }
}
