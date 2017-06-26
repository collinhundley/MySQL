//
//  DataType.swift
//  MySQL
//
//  Created by Collin Hundley on 6/25/17.
//

import Foundation


/// Defines the protocol for data types to be used as table column types.
public protocol DataType {
    /// Return database specific representation of the type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    static func create() -> String
}

/// SQL varchar type.
public struct Varchar: DataType {
    /// Return database specific representation of the varchar type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "varchar"
    }
}

/// SQL char/character type.
public struct Char: DataType {
    /// Return database specific representation of the char type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "char"
    }
}

/// SQL date type.
public struct SQLDate: DataType {
    /// Return database specific representation of the date type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "date"
    }
}

/// SQL time type.
public struct Time: DataType {
    /// Return database specific representation of the time type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "time"
    }
}

/// SQL timestamp type.
public struct Timestamp: DataType {
    /// Return database specific representation of the timestamp type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "timestamp"
    }
}

extension Int16: DataType {
    /// Return database specific representation of the int16 type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "smallint"
    }
}

extension Int32: DataType {
    /// Return database specific representation of the int32 type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "integer"
    }
}

extension Int64: DataType {
    /// Return database specific representation of the int32 type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "bigint"
    }
}

extension String: DataType {
    /// Return database specific representation of the string type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "text"
    }
}

extension Float: DataType {
    /// Return database specific representation of the float type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "float"
    }
}

extension Double: DataType {
    /// Return database specific representation of the double type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "double"
    }
}

extension Bool: DataType {
    /// Return database specific representation of the boolean type using `QueryBuilder`.
    ///
    /// - Returns: A String representation of the type.
    public static func create() -> String {
        return "boolean"
    }
}
