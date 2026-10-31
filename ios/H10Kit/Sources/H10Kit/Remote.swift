import Foundation

public struct Remote: Codable, Identifiable, Equatable {
    public static let homeServerOrigin = "https://home-server.example.com"
    public let id: String
    public var name: String
    public var origin: String
    public let installationId: String
    public var sending: Bool
    public var account: String?
    public var hasConnected: Bool?

    public init(id: String, name: String, origin: String, installationId: String, sending: Bool = true, account: String? = nil) {
        self.id = id; self.name = name; self.origin = origin; self.installationId = installationId
        self.sending = sending; self.account = account
    }

    public var url: URL? { URL(string: origin) }
    public var host: String { url?.host ?? origin }

    public func shouldQueueRecords(hasCredential: Bool) -> Bool { hasConnected == true || account != nil || hasCredential }

    public static func load(from defaults: UserDefaults) throws -> [Remote] {
        var remotes: [Remote] = []
        if let saved = defaults.object(forKey: "remotes") {
            guard let data = saved as? Data else { throw H10Error("Saved servers have an invalid format.") }
            remotes = try JSONDecoder().decode([Remote].self, from: data)
            for index in remotes.indices { remotes[index].migrateAddress() }
        } else if let installationId = defaults.string(forKey: "installationId") {
            remotes = [Remote(id: "legacy", name: "OpenPhys pilot server", origin: "https://sensors-poc.vineyard-finance.com",
                              installationId: installationId)]
        }
        try save(remotes, to: defaults)
        return remotes
    }

    public static func save(_ remotes: [Remote], to defaults: UserDefaults) throws {
        defaults.set(try JSONEncoder().encode(remotes), forKey: "remotes")
    }

    public mutating func migrateAddress() {
        let old = "https://home-server.tailnet.example:10443"
        if origin.lowercased() == old || origin.lowercased() == old + "/" { origin = Self.homeServerOrigin }
    }
}
