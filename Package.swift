// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "MySQL",
    dependencies: [
        .Package(url: "git@github.com:collinhundley/CMySQL.git", majorVersion: 2, minor: 0)
    ]
)
