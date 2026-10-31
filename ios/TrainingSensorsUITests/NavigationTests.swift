import XCTest

final class NavigationTests: XCTestCase {
    private var app: XCUIApplication!

    override func setUp() {
        continueAfterFailure = false
        app = XCUIApplication()
        app.launchArguments = ["-demo"]
        app.launch()
    }

    func testBottomTabsAndServerDetails() {
        app.tabBars.buttons["History"].tap()
        XCTAssertTrue(app.staticTexts["Heart rate"].waitForExistence(timeout: 5))
        app.tabBars.buttons["Device"].tap()
        XCTAssertTrue(app.staticTexts["Polar H10"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["Skin contact"].exists)
        app.swipeUp()
        XCTAssertTrue(app.buttons["disconnect-device"].isHittable)
        capture("device-disconnect")
        app.tabBars.buttons["Sync"].tap()
        app.buttons.matching(NSPredicate(format: "label CONTAINS %@", "Home server")).firstMatch.tap()
        XCTAssertTrue(app.staticTexts["Account"].waitForExistence(timeout: 5))
        let pause = app.buttons["pause-sync"]
        XCTAssertTrue(pause.exists)
        pause.tap()
        XCTAssertTrue(app.buttons["Resume"].waitForExistence(timeout: 3))
        app.buttons["Resume"].tap()
        XCTAssertTrue(app.buttons["Pause sync"].waitForExistence(timeout: 3))
        capture("server-detail")
    }

    func testFreshInstallWorksWithoutServersAcrossLaunches() {
        app.terminate()
        app.launchArguments = ["-tab", "3"]
        app.launch()
        XCTAssertTrue(app.buttons["Add server"].waitForExistence(timeout: 5))
        XCTAssertEqual(app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "remote-")).count, 0)
        capture("sync-without-servers")
        app.tabBars.buttons["Device"].tap()
        XCTAssertTrue(app.staticTexts["Polar H10"].waitForExistence(timeout: 3))
        app.tabBars.buttons["History"].tap()
        XCTAssertTrue(app.staticTexts["Heart rate"].waitForExistence(timeout: 3))
        app.terminate()
        app.launch()
        XCTAssertTrue(app.buttons["Add server"].waitForExistence(timeout: 5))
        XCTAssertEqual(app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "remote-")).count, 0)
    }

    func testHealthExportAsksForHeartRatePermission() {
        app.terminate()
        app.launchArguments = ["-tab", "3"]
        app.launch()
        let health = app.switches["health-export"]
        XCTAssertTrue(health.waitForExistence(timeout: 5))
        XCTAssertEqual(health.value as? String, "0")
        health.tap()
        XCTAssertTrue(app.staticTexts["Heart Rate"].waitForExistence(timeout: 10))
        capture("health-permission")
    }

    func testLowStorageWarningCanBeDismissedWithoutInterruptingRecording() {
        app.terminate()
        app.launchArguments = ["-demo", "-low-storage"]
        app.launch()
        let alert = app.alerts["Storage running low"]
        XCTAssertTrue(alert.waitForExistence(timeout: 5))
        XCTAssertTrue(alert.staticTexts["Free up space to keep recording. Unsent readings stay on this iPhone."].exists)
        capture("low-storage")
        alert.buttons["OK"].tap()
        app.tabBars.buttons["Device"].tap()
        XCTAssertTrue(app.staticTexts["Polar H10"].waitForExistence(timeout: 3))
        XCUIDevice.shared.press(.home)
        app.activate()
        XCTAssertTrue(app.staticTexts["Polar H10"].waitForExistence(timeout: 3))
        XCTAssertFalse(alert.exists)
    }

    func testInfoPopoverKeepsChartPosition() {
        let title = app.staticTexts["section-ECG"]
        XCTAssertTrue(title.waitForExistence(timeout: 5))
        let originalY = title.frame.minY
        app.buttons["About ECG"].tap()
        XCTAssertTrue(app.staticTexts["Your heart’s electrical activity."].waitForExistence(timeout: 3))
        XCTAssertEqual(title.frame.minY, originalY, accuracy: 1)
    }

    func testDeviceShortcutOpensDeviceTab() {
        app.buttons["open-device"].tap()
        XCTAssertTrue(app.staticTexts["Polar H10"].waitForExistence(timeout: 3))
        XCTAssertTrue(app.tabBars.buttons["Device"].isSelected)
    }

    func testEventCanBeCreatedAndRenamed() {
        app.swipeUp()
        XCTAssertTrue(app.staticTexts["X"].exists)
        XCTAssertTrue(app.staticTexts["Y"].exists)
        XCTAssertTrue(app.staticTexts["Z"].exists)
        app.buttons["add-mark"].tap()
        XCTAssertTrue(app.staticTexts["Events"].exists)
        capture("movement-and-marks")
        let mark = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "mark-")).firstMatch
        XCTAssertTrue(mark.waitForExistence(timeout: 3))
        mark.tap()
        let field = app.alerts.textFields.firstMatch
        XCTAssertTrue(field.waitForExistence(timeout: 3))
        field.tap()
        field.typeText(String(repeating: XCUIKeyboardKey.delete.rawValue, count: 5) + "Walk upstairs")
        app.alerts.buttons["Save"].tap()
        XCTAssertTrue(app.buttons.matching(NSPredicate(format: "label CONTAINS %@", "Walk upstairs")).firstMatch.waitForExistence(timeout: 3))
    }

    func testChartsAllowVerticalScrolling() {
        let title = app.staticTexts["section-ECG"]
        XCTAssertTrue(title.waitForExistence(timeout: 3))
        let originalY = title.frame.minY
        let start = app.coordinate(withNormalizedOffset: CGVector(dx: 0.6, dy: 0.78))
        let end = app.coordinate(withNormalizedOffset: CGVector(dx: 0.6, dy: 0.35))
        start.press(forDuration: 0.05, thenDragTo: end)
        XCTAssertLessThan(title.frame.minY, originalY - 50)
        capture("movement")
    }

    func testChartMarkCanBeRenamedAndDeleted() {
        let mark = app.buttons["chart-mark-demo"]
        XCTAssertTrue(mark.waitForExistence(timeout: 5))
        XCTAssertTrue(mark.label.contains("Walk"))
        mark.tap()
        let field = app.alerts.textFields.firstMatch
        XCTAssertTrue(field.waitForExistence(timeout: 3))
        field.tap()
        field.typeText(String(repeating: XCUIKeyboardKey.delete.rawValue, count: 4) + "Rest")
        app.alerts.buttons["Save"].tap()
        XCTAssertTrue(mark.label.contains("Rest"))
        app.swipeUp()
        let row = app.buttons["mark-demo"]
        XCTAssertTrue(row.waitForExistence(timeout: 3))
        row.swipeLeft()
        capture("event-delete")
        app.buttons["Delete"].tap()
        XCTAssertFalse(app.buttons["mark-demo"].exists)
        app.swipeDown()
        XCTAssertFalse(mark.exists)
    }

    func testDisconnectedDeviceShowsConnectAboveStatus() {
        app.terminate()
        app.launchArguments = ["-demo", "-disconnected", "-tab", "2"]
        app.launch()
        let connect = app.buttons["connect-device"]
        XCTAssertTrue(connect.waitForExistence(timeout: 5))
        XCTAssertTrue(connect.isEnabled)
        XCTAssertLessThan(connect.frame.maxY, app.staticTexts["Battery"].frame.minY)
        XCTAssertFalse(app.buttons["disconnect-device"].exists)
    }

    func testLostContactClearsHeartRate() {
        app.terminate()
        app.launchArguments = ["-demo", "-contact-lost"]
        app.launch()
        XCTAssertTrue(app.staticTexts["Check strap contact"].waitForExistence(timeout: 5))
        app.tabBars.buttons["Device"].tap()
        XCTAssertTrue(app.staticTexts["Lost"].exists)
        XCTAssertFalse(app.staticTexts["Detected"].exists)
    }

    func testBluetoothOffExplainsHowToRestoreRecording() {
        app.terminate()
        app.launchArguments = ["-demo", "-bluetooth-off"]
        app.launch()
        XCTAssertTrue(app.staticTexts["Bluetooth off"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["Turn it on in Control Center."].exists)
        XCTAssertFalse(app.buttons["bluetooth-settings"].exists)
        XCTAssertFalse(app.staticTexts["Connect your H10 in Device"].exists)
    }

    func testBluetoothDeniedOffersAppSettings() {
        app.terminate()
        app.launchArguments = ["-demo", "-bluetooth-denied"]
        app.launch()
        XCTAssertTrue(app.staticTexts["Bluetooth access off"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.buttons["bluetooth-settings"].isHittable)
    }

    func testStaleContactExpiresOnDeviceScreen() {
        app.terminate()
        app.launchArguments = ["-demo", "-stale-hr", "-tab", "2"]
        app.launch()
        let detected = app.staticTexts["Detected"]
        XCTAssertTrue(detected.waitForExistence(timeout: 3))
        XCTAssertTrue(detected.waitForNonExistence(timeout: 8))
        app.tabBars.buttons["Now"].tap()
        XCTAssertTrue(app.staticTexts["Waiting for heart rate"].exists)
    }

    func testServerConnectIsDisabledUntilFormIsComplete() {
        app.tabBars.buttons["Sync"].tap()
        app.buttons["Add server"].tap()
        let connect = app.buttons["connect-server"]
        XCTAssertTrue(connect.waitForExistence(timeout: 3))
        XCTAssertFalse(connect.isEnabled)
        for (placeholder, value) in [("https://server.example", "https://example.com"), ("Email", "test@example.com")] {
            app.textFields[placeholder].tap()
            app.textFields[placeholder].typeText(value)
        }
        app.secureTextFields["Password"].tap()
        app.secureTextFields["Password"].typeText("test-password")
        XCTAssertTrue(connect.isEnabled)
        app.swipeUp()
        capture("server-connect")
    }

    private func capture(_ name: String) {
        let attachment = XCTAttachment(screenshot: app.screenshot())
        attachment.name = name
        attachment.lifetime = .keepAlways
        add(attachment)
    }
}
