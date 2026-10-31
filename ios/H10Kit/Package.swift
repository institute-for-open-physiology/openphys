// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "H10Kit",
    platforms: [.iOS(.v17), .watchOS(.v10), .macOS(.v13)],
    products: [.library(name: "H10Kit", targets: ["H10Kit"])],
    targets: [
        .systemLibrary(name: "CZlib"),
        .target(name: "H10Kit", dependencies: ["CZlib"]),
        .testTarget(name: "H10KitTests", dependencies: ["H10Kit"]),
    ]
)
