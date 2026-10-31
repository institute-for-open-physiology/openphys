import Foundation
import Security

/// Stores device credentials. Readable after first unlock so background uploads keep working.
enum Keychain {
    private static func query(_ account: String) -> [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: "training-sensors",
            kSecAttrAccount as String: account,
        ]
    }

    static func read(_ account: String) -> String? {
        var request = query(account)
        request[kSecReturnData as String] = true
        request[kSecMatchLimit as String] = kSecMatchLimitOne
        var result: CFTypeRef?
        guard SecItemCopyMatching(request as CFDictionary, &result) == errSecSuccess, let data = result as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }

    @discardableResult
    static func write(_ account: String, _ value: String?) -> Bool {
        let request = query(account)
        guard let value else {
            let result = SecItemDelete(request as CFDictionary)
            return result == errSecSuccess || result == errSecItemNotFound
        }
        let attributes: [String: Any] = [kSecValueData as String: Data(value.utf8),
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly]
        let result = SecItemUpdate(request as CFDictionary, attributes as CFDictionary)
        if result == errSecSuccess { return true }
        guard result == errSecItemNotFound else { return false }
        return SecItemAdd(request.merging(attributes) { _, new in new } as CFDictionary, nil) == errSecSuccess
    }
}
