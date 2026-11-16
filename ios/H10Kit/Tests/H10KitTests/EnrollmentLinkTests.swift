import Foundation
import XCTest
@testable import H10Kit

final class EnrollmentLinkTests: XCTestCase {
    private let token = String(repeating: "a", count: 43)

    func testAppLinkPreservesServerPortAndToken() throws {
        let url = try XCTUnwrap(URL(string: "openphys://example.com:10443/enroll#" + token))
        let connection = try XCTUnwrap(EnrollmentLink.parseAppURL(url))
        XCTAssertEqual(connection.origin.absoluteString, "https://example.com:10443")
        XCTAssertEqual(connection.link.absoluteString, "https://example.com:10443/enroll#" + token)
        XCTAssertEqual(EnrollmentLink.token(from: url.absoluteString, origin: connection.origin), token)
        XCTAssertNil(EnrollmentLink.token(from: url.absoluteString, origin: URL(string: "https://other.example:10443")!))
    }

    func testAppLinkRejectsMalformedAndUnrelatedURLs() throws {
        for value in [
            "https://example.com/enroll#" + token,
            "openphys:///enroll#" + token,
            "openphys://user:password@example.com/enroll#" + token,
            "openphys://example.com:0/enroll#" + token,
            "openphys://example.com:65536/enroll#" + token,
            "openphys://example.com/other#" + token,
            "openphys://example.com/enroll?token=" + token,
            "openphys://example.com/enroll#short",
            "openphys://example.com/enroll",
        ] {
            let url = try XCTUnwrap(URL(string: value))
            XCTAssertNil(EnrollmentLink.parseAppURL(url), value)
        }
    }
}
