import XCTest
import UIKit

final class SnapshotTests: XCTestCase {
    struct Section: Decodable { let title: String }
    struct Screen: Decodable { let title: String; let order: Int; let sections: [String: Section] }

    func testSharedScreens() throws {
        let bundle = Bundle(for: Self.self)
        let urls = try XCTUnwrap(bundle.urls(forResourcesWithExtension: "json", subdirectory: "screens"))
        let screens = try urls.map { try JSONDecoder().decode(Screen.self, from: Data(contentsOf: $0)) }.sorted { $0.order < $1.order }
        XCTAssertFalse(screens.isEmpty)
        for mode in ["dark", "light"] {
            for screen in screens {
                let app = XCUIApplication()
                app.launchArguments = ["-demo", "-snapshot", "-appearance", mode, "-tab", String(screen.order)]
                app.launchEnvironment["TZ"] = "UTC"
                app.launch()
                XCTAssertTrue(app.tabBars.buttons[screen.title].waitForExistence(timeout: 5))
                XCTAssertTrue(app.tabBars.buttons[screen.title].isSelected)
                let name = "\(screen.title)-\(mode)"
                let actual = app.screenshot().image
                let attachment = XCTAttachment(image: actual)
                attachment.name = name
                attachment.lifetime = .keepAlways
                add(attachment)
                if let url = bundle.url(forResource: name, withExtension: "png", subdirectory: "Snapshots"),
                   let expected = UIImage(contentsOfFile: url.path) {
                    compare(actual, expected, name: name)
                } else {
                    XCTFail("Missing baseline \(name). Inspect the attached screenshot before recording it.")
                }
                var remaining = Set(screen.sections.values.map { "section-" + $0.title })
                for _ in 0..<10 {
                    remaining = remaining.filter { !app.staticTexts[$0].isHittable }
                    if remaining.isEmpty { break }
                    let from = app.coordinate(withNormalizedOffset: CGVector(dx: 0.95, dy: 0.8))
                    let to = app.coordinate(withNormalizedOffset: CGVector(dx: 0.95, dy: 0.25))
                    from.press(forDuration: 0.05, thenDragTo: to)
                }
                XCTAssertTrue(remaining.isEmpty, "Missing sections: \(remaining.sorted())")
                app.terminate()
            }
        }
    }

    private func compare(_ actual: UIImage, _ expected: UIImage, name: String) {
        guard let a = actual.cgImage, let b = expected.cgImage else { return XCTFail("Missing pixels: \(name)") }
        XCTAssertEqual(a.width, b.width, name)
        XCTAssertEqual(a.height, b.height, name)
        guard a.width == b.width, a.height == b.height else { return }
        func pixels(_ image: CGImage) -> [UInt8] {
            var data = [UInt8](repeating: 0, count: image.width * image.height * 4)
            let context = CGContext(data: &data, width: image.width, height: image.height, bitsPerComponent: 8,
                bytesPerRow: image.width * 4, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
            context.draw(image, in: CGRect(x: 0, y: 0, width: image.width, height: image.height))
            return data
        }
        let left = pixels(a), right = pixels(b)
        // The status bar belongs to the OS. Compare the app below its safe area.
        let skip = Int(64 * actual.scale) * a.width * 4
        var changed = 0
        for i in stride(from: skip, to: left.count, by: 4) {
            if (0..<3).contains(where: { abs(Int(left[i + $0]) - Int(right[i + $0])) > 12 }) { changed += 1 }
        }
        let fraction = Double(changed) / Double((left.count - skip) / 4)
        XCTAssertLessThan(fraction, 0.003, "\(name): \(Int(fraction * 10000)) / 10000 pixels changed")
    }
}
