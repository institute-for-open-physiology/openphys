import Foundation

/// Ordered JSON value with deterministic encoding, so retried batch bytes and checksums never change.
public indirect enum JSONValue {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([(String, JSONValue)])
    /// Pre-encoded JSON, inserted verbatim.
    case raw(String)

    public static func str(_ value: String?) -> JSONValue { value.map(JSONValue.string) ?? .null }
    public static func num<T: BinaryInteger>(_ value: T) -> JSONValue { .int(Int64(clamping: value)) }
    public static func optNum<T: BinaryInteger>(_ value: T?) -> JSONValue { value.map { .int(Int64(clamping: $0)) } ?? .null }

    public var encoded: String {
        var out = ""
        write(to: &out)
        return out
    }

    private func write(to out: inout String) {
        switch self {
        case .null: out += "null"
        case .bool(let value): out += value ? "true" : "false"
        case .int(let value): out += String(value)
        case .double(let value): out += value.isFinite ? String(value) : "null"
        case .string(let value): JSONValue.writeString(value, to: &out)
        case .raw(let value): out += value
        case .array(let items):
            out += "["
            for (index, item) in items.enumerated() {
                if index > 0 { out += "," }
                item.write(to: &out)
            }
            out += "]"
        case .object(let fields):
            out += "{"
            for (index, field) in fields.enumerated() {
                if index > 0 { out += "," }
                JSONValue.writeString(field.0, to: &out)
                out += ":"
                field.1.write(to: &out)
            }
            out += "}"
        }
    }

    private static func writeString(_ value: String, to out: inout String) {
        out += "\""
        for scalar in value.unicodeScalars {
            switch scalar {
            case "\"": out += "\\\""
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            default:
                if scalar.value < 0x20 {
                    let hex = String(scalar.value, radix: 16)
                    out += "\\u" + String(repeating: "0", count: 4 - hex.count) + hex
                } else {
                    out.unicodeScalars.append(scalar)
                }
            }
        }
        out += "\""
    }
}

extension JSONValue: ExpressibleByStringLiteral, ExpressibleByStringInterpolation, ExpressibleByIntegerLiteral,
    ExpressibleByFloatLiteral, ExpressibleByBooleanLiteral, ExpressibleByNilLiteral,
    ExpressibleByArrayLiteral, ExpressibleByDictionaryLiteral
{
    public init(stringLiteral value: String) { self = .string(value) }
    public init(integerLiteral value: Int64) { self = .int(value) }
    public init(floatLiteral value: Double) { self = .double(value) }
    public init(booleanLiteral value: Bool) { self = .bool(value) }
    public init(nilLiteral: ()) { self = .null }
    public init(arrayLiteral elements: JSONValue...) { self = .array(elements) }
    public init(dictionaryLiteral elements: (String, JSONValue)...) { self = .object(elements) }
}
