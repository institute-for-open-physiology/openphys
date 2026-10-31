import Foundation

public struct StorageWarning {
    public private(set) var warned: Bool

    public init(warned: Bool = false) { self.warned = warned }

    /// Hysteresis prevents repeated alerts when free space hovers near one GiB.
    public mutating func check(availableBytes: Int64?) -> Bool {
        guard let availableBytes, availableBytes >= 0 else { return false }
        if availableBytes >= 1280 * 1024 * 1024 { warned = false }
        guard availableBytes < 1024 * 1024 * 1024, !warned else { return false }
        warned = true
        return true
    }
}
