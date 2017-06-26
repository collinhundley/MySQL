# MySQL
A simple MySQL client for Swift


This client provides a Swift interface for the MySQL C library.

On macOS, the compiler must know where to find the C lib. To generate an Xcode project:

```sh
swift package -Xlinker -L/usr/local/lib generate-xcodeproj
```
