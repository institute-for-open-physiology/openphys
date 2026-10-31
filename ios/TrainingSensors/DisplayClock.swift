import Foundation
import QuartzCore
import SwiftUI

enum DisplayClock {
    static var snapshot: Bool {
        #if DEBUG
        ProcessInfo.processInfo.arguments.contains("-snapshot")
        #else
        false
        #endif
    }
    static var appearance: ColorScheme? {
        #if DEBUG
        let args = ProcessInfo.processInfo.arguments
        if let index = args.firstIndex(of: "-appearance"), args.indices.contains(index + 1) {
            return args[index + 1] == "dark" ? .dark : .light
        }
        #endif
        return nil
    }
    static var uptime: Double { snapshot ? 10_000 : CACurrentMediaTime() }
    static func date(_ live: Date = Date()) -> Date {
        #if DEBUG
        if snapshot { return Date(timeIntervalSince1970: DashboardFixture.load().time) }
        #endif
        return live
    }
}

#if DEBUG
struct DashboardFixture: Decodable {
    struct Event: Decodable { let id: String; let time: Double; let label: String }
    let time: Double
    let bpm: Int
    let battery: Int
    let rssi: Int
    let rr: Double
    let acceleration: [Double]
    let history: [[Double]]
    let heartRate: [[Double]]
    let ecg: [[Double]]
    let events: [Event]
    private static let fixture: DashboardFixture = {
        let url = Bundle.main.url(forResource: "dashboard", withExtension: "json", subdirectory: "shared/fixtures")!
        return try! JSONDecoder().decode(DashboardFixture.self, from: Data(contentsOf: url))
    }()
    static func load() -> DashboardFixture { fixture }
}
#endif
