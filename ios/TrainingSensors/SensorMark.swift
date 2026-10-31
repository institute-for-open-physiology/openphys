import Foundation
import H10Kit

enum MarkStorage {
    static let url = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0].appendingPathComponent("marks.json")
    static func load() throws -> MarkArchive {
        if FileManager.default.fileExists(atPath: url.path) { return try JSONDecoder().decode(MarkArchive.self, from: Data(contentsOf: url)) }
        let data = UserDefaults.standard.data(forKey: "sensorMarks")
        let marks = try data.map { try JSONDecoder().decode([SensorMark].self, from: $0) } ?? []
        let archive = MarkArchive(marks: marks)
        try archive.write(to: url)
        return archive
    }
}
