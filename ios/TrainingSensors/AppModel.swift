import Foundation
import H10Kit
import UIKit

struct RemoteStatus: Equatable {
    var left = 0
    var problem: String?
    var acknowledgedAt: Date?
}

@MainActor
final class AppModel: ObservableObject {
    #if DEBUG
    static let defaultServers = [("OpenPhys pilot server", "https://sensors-poc.vineyard-finance.com"),
                                 ("Home server", Remote.homeServerOrigin)]
    #endif
    private static let legacyAccount = "device-credential"

    @Published private(set) var remotes: [Remote] = []
    @Published private(set) var status: [String: RemoteStatus] = [:]
    @Published private(set) var signedIn: Set<String> = []
    @Published private(set) var remoteError: String?
    @Published private(set) var preparingQueues = true
    @Published var showingStorageWarning = false
    private var storageWarning = StorageWarning(warned: UserDefaults.standard.bool(forKey: "storageWarningShown"))

    let collector: H10Collector
    let health = HealthExport()
    private var credentials: [String: String] = [:]
    private var stores: [String: RecordStore] = [:]
    private var uploads: [String: Task<Void, Never>] = [:]
    private var uploadWaits: [String: Task<Void, Never>] = [:]
    private var watch: WatchCompanion?

    init() {
        collector = H10Collector()
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("-demo") {
            remotes = Self.defaultServers.map { Remote(id: Ids.new(), name: $0.0, origin: $0.1, installationId: Ids.new()) }
            signedIn = Set(remotes.map(\.id))
            for remote in remotes { status[remote.id] = RemoteStatus(acknowledgedAt: DisplayClock.snapshot ? DisplayClock.date() : Date()) }
            collector.startDemo()
            preparingQueues = false
            return
        }
        #endif
        do { remotes = try Remote.load(from: .standard) }
        catch { remoteError = "Could not open saved servers. Your recordings are still on this iPhone." }
        collector.onHeartRate = { [weak health] sample, time, serial in health?.record(sample, at: time, serial: serial) }
        health.resume()
        Task {
            for remote in remotes { await open(remote) }
            routeRecords()
            preparingQueues = false
            collector.resume()
            watch = WatchCompanion(collector: collector)
        }
    }

    func summary(_ remote: Remote) -> (level: Level, word: String) {
        let state = status[remote.id] ?? RemoteStatus()
        if !signedIn.contains(remote.id) { return (.none, "Signed out") }
        if !remote.sending { return (.none, "Paused") }
        if state.problem != nil { return (.bad, "Retrying") }
        if let acknowledged = state.acknowledgedAt, Date().timeIntervalSince(acknowledged) < 10 {
            return (.good, "Syncing")
        }
        if state.left == 0 { return (state.acknowledgedAt == nil ? .none : .good, "Up to date") }
        return (state.acknowledgedAt == nil ? .none : .fair, "Waiting")
    }

    func isTransferring(_ remote: Remote, at now: Date) -> Bool {
        guard remote.sending, signedIn.contains(remote.id), status[remote.id]?.problem == nil,
              let acknowledged = status[remote.id]?.acknowledgedAt else { return false }
        return now.timeIntervalSince(acknowledged) < 3
    }

    func setSending(_ id: String, _ sending: Bool) {
        update(id) { $0.sending = sending }
        uploadWaits[id]?.cancel()
    }

    func resume() {
        health.resume()
        for wait in uploadWaits.values { wait.cancel() }
    }

    func checkStorage() async {
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("-demo") {
            if ProcessInfo.processInfo.arguments.contains("-low-storage"), storageWarning.check(availableBytes: 0) { showingStorageWarning = true }
            return
        }
        #endif
        let directory = Self.supportDirectory
        let available = await Task.detached(priority: .utility) {
            try? directory.resourceValues(forKeys: [.volumeAvailableCapacityForImportantUsageKey]).volumeAvailableCapacityForImportantUsage
        }.value
        guard !Task.isCancelled else { return }
        if storageWarning.check(availableBytes: available) { showingStorageWarning = true }
        if UserDefaults.standard.bool(forKey: "storageWarningShown") != storageWarning.warned {
            UserDefaults.standard.set(storageWarning.warned, forKey: "storageWarningShown")
        }
    }

    func signIn(_ id: String, email: String, password: String) async -> String? {
        guard let remote = remotes.first(where: { $0.id == id }) else { return "This server was removed." }
        do {
            try await enroll(remote, email: email, password: password)
        } catch {
            return Self.message(error)
        }
        update(id) { $0.account = email; $0.sending = true }
        routeRecords()
        uploadWaits[id]?.cancel()
        return nil
    }

    /// Adds a server only after signing in to it, so a failed attempt leaves nothing behind.
    func add(name: String, address: String, email: String, password: String) async -> String? {
        if let remoteError { return remoteError }
        var origin = address.trimmingCharacters(in: .whitespacesAndNewlines)
        if !origin.contains("://") { origin = "https://" + origin }
        while origin.hasSuffix("/") { origin.removeLast() }
        guard let url = URL(string: origin), let host = url.host, url.scheme == "https",
              url.user == nil, url.password == nil, url.query == nil, url.fragment == nil,
              url.path.isEmpty else { return "Enter an HTTPS server address without a path." }
        guard !remotes.contains(where: { $0.origin.lowercased() == origin.lowercased() }) else { return "This server is already added." }
        let label = name.trimmingCharacters(in: .whitespacesAndNewlines)
        let remote = Remote(id: Ids.new(), name: label.isEmpty ? host : label, origin: origin, installationId: Ids.new(), account: email)
        do {
            try await enroll(remote, email: email, password: password)
        } catch {
            return Self.message(error)
        }
        remotes.append(remote)
        Self.save(remotes)
        await open(remote)
        routeRecords()
        return nil
    }

    @discardableResult
    func signOut(_ id: String) -> Bool {
        guard Keychain.write(Self.account(id), nil) else { setProblem(id, "Unlock your iPhone and try signing out again."); return false }
        credentials[id] = nil
        signedIn.remove(id)
        routeRecords()
        return true
    }

    @discardableResult
    func remove(_ id: String) -> Bool {
        guard signOut(id) else { return false }
        uploads.removeValue(forKey: id)?.cancel()
        uploadWaits.removeValue(forKey: id)?.cancel()
        stores[id] = nil
        status[id] = nil
        remotes.removeAll { $0.id == id }
        Self.save(remotes)
        routeRecords()
        try? FileManager.default.removeItem(at: Self.queueDirectory(id))
        return true
    }

    // MARK: Setup

    private func open(_ remote: Remote) async {
        if let credential = Keychain.read(Self.account(remote.id)) {
            credentials[remote.id] = credential
            signedIn.insert(remote.id)
            if remote.account == nil, remote.hasConnected != true { update(remote.id) { $0.hasConnected = true } }
        }
        do {
            let directory = Self.queueDirectory(remote.id)
            let store = try await Task.detached(priority: .userInitiated) {
                try RecordStore(directory: directory, installationId: remote.installationId)
            }.value
            guard remotes.contains(where: { $0.id == remote.id }) else { return }
            stores[remote.id] = store
        } catch {
            status[remote.id] = RemoteStatus(problem: "Cannot open the local queue.")
            return
        }
        uploads[remote.id] = Task { [weak self] in await self?.upload(remote.id) }
    }

    /// Once connected, servers keep recording through network outages and paused sync.
    private func routeRecords() {
        collector.setStores(remotes.compactMap { $0.shouldQueueRecords(hasCredential: signedIn.contains($0.id)) ? stores[$0.id] : nil })
    }

    private func update(_ id: String, _ change: (inout Remote) -> Void) {
        guard let index = remotes.firstIndex(where: { $0.id == id }) else { return }
        change(&remotes[index])
        Self.save(remotes)
    }

    private static func save(_ remotes: [Remote]) {
        try? Remote.save(remotes, to: .standard)
    }

    private static var supportDirectory: URL { FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0] }
    private static func queueDirectory(_ id: String) -> URL { supportDirectory.appendingPathComponent(id == "legacy" ? "queue" : "queue-" + id, isDirectory: true) }
    private static func account(_ id: String) -> String { id == "legacy" ? legacyAccount : "device-credential-" + id }

    // MARK: Enrollment

    /// Signs in like the website, creates a one-use enrollment and exchanges it for a device credential.
    private func enroll(_ remote: Remote, email: String, password: String) async throws {
        guard let server = remote.url else { throw H10Error("invalid server address") }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpCookieAcceptPolicy = .never
        configuration.httpShouldSetCookies = false
        let session = URLSession(configuration: configuration)
        let (_, login) = try await post(session, remote.origin, "/api/v1/auth/password/login",
                                        body: ["email": .string(email), "password": .string(password)], browser: true)
        let headers = login.allHeaderFields.reduce(into: [String: String]()) { result, pair in
            if let key = pair.key as? String, let value = pair.value as? String { result[key] = value }
        }
        let cookies = HTTPCookie.cookies(withResponseHeaderFields: headers, for: server)
        guard let csrf = cookies.first(where: { $0.name == "sensors_csrf" })?.value else {
            throw H10Error("sign-in did not return a session")
        }
        let (data, _) = try await post(session, remote.origin, "/api/v1/device-enrollments", body: [:], browser: true, cookies: cookies, csrf: csrf)
        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let url = json?["enrollment_url"] as? String, let token = EnrollmentLink.token(from: url, origin: server) else {
            throw H10Error("server returned no connection link")
        }
        // Logging out revokes unused enrollment links.
        try await exchange(token, remote)
        _ = try? await post(session, remote.origin, "/api/v1/auth/logout", body: [:], browser: true, cookies: cookies, csrf: csrf)
    }

    private func exchange(_ token: String, _ remote: Remote) async throws {
        let info = Bundle.main.infoDictionary
        let version = info?["CFBundleShortVersionString"] as? String ?? "0"
        let build = Int(info?["CFBundleVersion"] as? String ?? "") ?? 1
        let body: JSONValue = [
            "enrollment_token": .string(token),
            "installation_id": .string(remote.installationId),
            "exchange_id": .string(Ids.new()),
            "display_label": .string(String(UIDevice.current.name.prefix(40))),
            "android_manufacturer": "Apple",
            "android_model": .string(H10Collector.model),
            "android_sdk": .num(ProcessInfo.processInfo.operatingSystemVersion.majorVersion),
            "app_package": .string(Bundle.main.bundleIdentifier ?? "com.vineyardfinance.sensors.ios"),
            "app_version_name": .string("ios-" + version),
            "app_version_code": .num(build),
        ]
        let (data, _) = try await post(URLSession(configuration: .ephemeral), remote.origin, "/api/v1/device-enrollments/exchange", body: body)
        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard (json?["installation_id"] as? String)?.lowercased() == remote.installationId else {
            throw H10Error("enrollment installation mismatch")
        }
        guard let credential = json?["device_credential"] as? String, EnrollmentLink.isToken(credential) else {
            throw H10Error("invalid device credential response")
        }
        guard Keychain.write(Self.account(remote.id), credential) else {
            throw H10Error("Could not save the connection securely. Try again after unlocking your iPhone.")
        }
        credentials[remote.id] = credential
        signedIn.insert(remote.id)
    }

    private static func request(_ origin: String, _ path: String) -> URLRequest {
        var request = URLRequest(url: URL(string: origin + path)!)
        request.httpMethod = "POST"
        request.timeoutInterval = 30
        request.setValue("application/json; charset=utf-8", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue("no-store", forHTTPHeaderField: "Cache-Control")
        return request
    }

    private func post(_ session: URLSession, _ origin: String, _ path: String, body: JSONValue, browser: Bool = false,
                      cookies: [HTTPCookie] = [], csrf: String? = nil) async throws -> (Data, HTTPURLResponse) {
        var request = Self.request(origin, path)
        if browser { request.setValue(origin, forHTTPHeaderField: "Origin") }
        if let csrf { request.setValue(csrf, forHTTPHeaderField: "x-csrf-token") }
        for (name, value) in HTTPCookie.requestHeaderFields(with: cookies) { request.setValue(value, forHTTPHeaderField: name) }
        request.httpBody = Data(body.encoded.utf8)
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw H10Error("no HTTP response") }
        guard (200..<300).contains(http.statusCode) else {
            let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
            throw H10Error(json?["error"] as? String ?? "HTTP \(http.statusCode)")
        }
        return (data, http)
    }

    // MARK: Upload

    private func upload(_ id: String) async {
        var backoff = 0.25
        while !Task.isCancelled {
            guard let store = stores[id], let remote = remotes.first(where: { $0.id == id }) else { return }
            setLeft(id, store.backlog().records)
            if let failure = store.lastFailure {
                setProblem(id, failure)
                do { try store.resumeWrites() }
                catch { await pause(30, for: id); continue }
            }
            guard remote.sending, let credential = credentials[id] else {
                await pause(60, for: id)
                continue
            }
            do {
                guard let batch = try store.nextBatch() else {
                    setProblem(id, nil)
                    backoff = 0.25
                    await pause(uploadInterval, for: id)
                    continue
                }
                let moreQueued = store.backlog().records > batch.recordCount
                var request = Self.request(remote.origin, "/api/v1/ingest/batches")
                request.setValue("Device " + credential, forHTTPHeaderField: "Authorization")
                request.httpBody = Data(batch.body.utf8)
                let (data, response) = try await URLSession.shared.data(for: request)
                let code = (response as? HTTPURLResponse)?.statusCode ?? 0
                if (code == 401 || code == 403) && IngestAck.machineCode(data) == "device_reconnect_required" {
                    if credentials[id] == credential { credentials[id] = nil; signedIn.remove(id) }
                    setProblem(id, "Sign in again to resume.")
                    continue
                }
                if code == 400, IngestAck.machineCode(data) == "invalid_request", try store.recoverRejectedBatch(batch) {
                    continue
                }
                guard (200..<300).contains(code) else {
                    throw H10Error("HTTP \(code)" + (IngestAck.machineCode(data).map { " " + $0 } ?? ""))
                }
                try store.acknowledge(batch, committedThrough: try IngestAck.committedThrough(responseBody: data, batch: batch))
                status[id, default: RemoteStatus()].acknowledgedAt = Date()
                setProblem(id, nil)
                backoff = 0.25
                await pause(moreQueued ? 0.15 : uploadInterval, for: id)
            } catch {
                guard !Task.isCancelled else { return }
                setProblem(id, Self.message(error))
                await pause(backoff + Double.random(in: 0...0.25), for: id)
                backoff = min(backoff * 2, 30)
            }
        }
    }

    private func setLeft(_ id: String, _ left: Int) {
        if status[id, default: RemoteStatus()].left != left { status[id, default: RemoteStatus()].left = left }
    }

    private func setProblem(_ id: String, _ problem: String?) {
        if status[id, default: RemoteStatus()].problem != problem { status[id, default: RemoteStatus()].problem = problem }
    }

    private var uploadInterval: Double {
        UIApplication.shared.applicationState == .active ? 1 : 15
    }

    private func pause(_ seconds: Double, for id: String) async {
        guard !Task.isCancelled else { return }
        let wait = Task { _ = try? await Task.sleep(for: .seconds(seconds)) }
        uploadWaits[id] = wait
        await withTaskCancellationHandler { _ = await wait.value } onCancel: { wait.cancel() }
        uploadWaits[id] = nil
    }

    static func message(_ error: Error) -> String {
        String(((error as? H10Error)?.message ?? error.localizedDescription).prefix(240))
    }
}
