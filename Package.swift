// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "MySQL",
    dependencies: [
        .Package(url: "https://github.com/collinhundley/CMySQL.git", majorVersion: 2, minor: 0)
    ]
)
