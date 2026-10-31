import XCTest
@testable import H10Kit

final class StorageWarningTests: XCTestCase {
    func testWarningAppearsOnceAndSurvivesRestart() {
        var warning = StorageWarning()
        XCTAssertTrue(warning.check(availableBytes: 500 * 1024 * 1024))
        XCTAssertFalse(warning.check(availableBytes: 0))
        var reopened = StorageWarning(warned: warning.warned)
        XCTAssertFalse(reopened.check(availableBytes: 500 * 1024 * 1024))
    }

    func testWarningRearmsOnlyAfterSpaceRecovers() {
        var warning = StorageWarning(warned: true)
        XCTAssertFalse(warning.check(availableBytes: 1100 * 1024 * 1024))
        XCTAssertFalse(warning.check(availableBytes: 900 * 1024 * 1024))
        XCTAssertFalse(warning.check(availableBytes: 1400 * 1024 * 1024))
        XCTAssertFalse(warning.warned)
        XCTAssertTrue(warning.check(availableBytes: 900 * 1024 * 1024))
    }

    func testUnavailableCapacityDoesNotWarnOrRearm() {
        var warning = StorageWarning()
        XCTAssertFalse(warning.check(availableBytes: nil))
        XCTAssertFalse(warning.check(availableBytes: -1))
        XCTAssertFalse(warning.warned)
        XCTAssertTrue(warning.check(availableBytes: 0))
        XCTAssertFalse(warning.check(availableBytes: nil))
        XCTAssertTrue(warning.warned)
    }
}
