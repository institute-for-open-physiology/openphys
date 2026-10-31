import XCTest
@testable import H10Kit

final class RemoteTests: XCTestCase {
    private var defaults: UserDefaults!
    private var suite: String!

    override func setUp() {
        suite = "TrainingSensors.RemoteTests." + UUID().uuidString
        defaults = UserDefaults(suiteName: suite)
    }

    override func tearDown() { defaults.removePersistentDomain(forName: suite) }

    func testFreshInstallHasNoServersAcrossLaunches() throws {
        XCTAssertTrue(try Remote.load(from: defaults).isEmpty)
        XCTAssertTrue(try Remote.load(from: defaults).isEmpty)
        XCTAssertNil(defaults.string(forKey: "installationId"))
    }

    func testSavedServersKeepTheirSettingsAndIdentities() throws {
        let saved = [Remote(id: "legacy", name: "OpenPhys pilot server", origin: "https://sensors-poc.vineyard-finance.com",
                            installationId: "first", account: "tester@example.com"),
                     Remote(id: "second", name: "Home", origin: Remote.homeServerOrigin,
                            installationId: "second-installation", sending: false, account: "home@example.com")]
        try Remote.save(saved, to: defaults)
        XCTAssertEqual(try Remote.load(from: defaults), saved)
        XCTAssertEqual(try Remote.load(from: defaults), saved)
    }

    func testSingleServerInstallKeepsLegacyQueueAndInstallationId() throws {
        defaults.set("existing-installation", forKey: "installationId")
        let saved = try Remote.load(from: defaults)
        XCTAssertEqual(saved.count, 1)
        XCTAssertEqual(saved[0].id, "legacy")
        XCTAssertEqual(saved[0].installationId, "existing-installation")
        XCTAssertEqual(saved[0].origin, "https://sensors-poc.vineyard-finance.com")
        XCTAssertTrue(saved[0].shouldQueueRecords(hasCredential: true))
        XCTAssertEqual(try Remote.load(from: defaults), saved)
    }

    func testRemovingAllServersDoesNotRecreateLegacyServer() throws {
        defaults.set("existing-installation", forKey: "installationId")
        try Remote.save([], to: defaults)
        XCTAssertTrue(try Remote.load(from: defaults).isEmpty)
    }

    func testUnreadableSettingsStayIntact() throws {
        let data = Data("incomplete settings".utf8)
        defaults.set(data, forKey: "remotes")
        XCTAssertThrowsError(try Remote.load(from: defaults))
        XCTAssertEqual(defaults.data(forKey: "remotes"), data)
    }

    func testWrongTypeSettingsStayIntact() {
        defaults.set("saved server settings", forKey: "remotes")
        XCTAssertThrowsError(try Remote.load(from: defaults))
        XCTAssertEqual(defaults.string(forKey: "remotes"), "saved server settings")
    }

    func testUnconfiguredServersDoNotQueueButConnectedServersKeepRecordingOffline() {
        var remote = Remote(id: "legacy", name: "Server", origin: "https://example.com", installationId: "installation")
        XCTAssertFalse(remote.shouldQueueRecords(hasCredential: false))
        XCTAssertTrue(remote.shouldQueueRecords(hasCredential: true))
        remote.account = "tester@example.com"
        remote.sending = false
        XCTAssertTrue(remote.shouldQueueRecords(hasCredential: false))
    }

    func testLegacyConnectionKeepsRecordingAfterSignOutAndRelaunch() throws {
        var remote = Remote(id: "legacy", name: "Server", origin: "https://example.com", installationId: "installation")
        remote.hasConnected = true
        try Remote.save([remote], to: defaults)
        let saved = try Remote.load(from: defaults)[0]
        XCTAssertTrue(saved.shouldQueueRecords(hasCredential: false))
        XCTAssertNil(saved.account)
    }

    func testHomeServerMigrationPreservesAccountAndQueueIdentity() throws {
        let data = Data(#"[{"id":"saved","name":"Home","origin":"https://home-server.tailnet.example:10443/","installationId":"installation","sending":false,"account":"test@example.com"}]"#.utf8)
        var remote = try JSONDecoder().decode([Remote].self, from: data)[0]
        let before = remote
        remote.migrateAddress()
        XCTAssertEqual(remote.origin, Remote.homeServerOrigin)
        XCTAssertEqual(remote.id, before.id)
        XCTAssertEqual(remote.installationId, before.installationId)
        XCTAssertEqual(remote.account, before.account)
        XCTAssertEqual(remote.name, before.name)
        XCTAssertEqual(remote.sending, before.sending)
        let migrated = remote
        remote.migrateAddress()
        XCTAssertEqual(remote, migrated)
    }

    func testMigrationLeavesOtherServersUntouched() {
        for origin in ["https://sensors-poc.vineyard-finance.com", "https://example.com", "https://home-server.tailnet.example:8443"] {
            var remote = Remote(id: "saved", name: "Server", origin: origin, installationId: "installation")
            remote.migrateAddress()
            XCTAssertEqual(remote.origin, origin)
        }
    }
}
