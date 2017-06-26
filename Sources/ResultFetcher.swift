//
//  ResultFetcher.swift
//  MySQL
//
//  Created by Collin Hundley on 6/24/17.
//

import Foundation
#if os(Linux)
    import CMySQLLinux
#else
    import CMySQLMac
#endif


/// An implementation of query result fetcher.
public class ResultFetcher {
    
    enum Error: Swift.Error, LocalizedError {
        case database(String)
        
        var errorDescription: String? {
            switch self {
            case .database(let msg):
                return msg
            }
        }
    }
    
    
    private var preparedStatement: PreparedStatement
    private var bindPtr: UnsafeMutablePointer<MYSQL_BIND>?
    private let binds: [MYSQL_BIND]
    
    private let fieldNames: [String]
    
    private var hasMoreRows = true
    
    
    init(preparedStatement: PreparedStatement, resultMetadata: UnsafeMutablePointer<MYSQL_RES>) throws {
        guard let fields = mysql_fetch_fields(resultMetadata) else {
            throw ResultFetcher.initError(preparedStatement)
        }
        
        let numFields = Int(mysql_num_fields(resultMetadata))
        var binds = [MYSQL_BIND]()
        var fieldNames = [String]()
        
        for i in 0 ..< numFields {
            let field = fields[i]
            binds.append(ResultFetcher.getOutputBind(field))
            fieldNames.append(String(cString: field.name))
        }
        
        let bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
        for i in 0 ..< binds.count {
            bindPtr[i] = binds[i]
        }
        
        guard mysql_stmt_bind_result(preparedStatement.statement, bindPtr) == 0 else {
            throw ResultFetcher.initError(preparedStatement, bindPtr: bindPtr, binds: binds)
        }
        
        guard mysql_stmt_execute(preparedStatement.statement) == 0 else {
            throw ResultFetcher.initError(preparedStatement, bindPtr: bindPtr, binds: binds)
        }
        
        self.preparedStatement = preparedStatement
        self.bindPtr = bindPtr
        self.binds = binds
        self.fieldNames = fieldNames
    }
    
    deinit {
        close()
    }
    
    private static func initError(_ preparedStatement: PreparedStatement, bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil, binds: [MYSQL_BIND]? = nil) -> Error {
        
        defer {
            preparedStatement.release()
        }
        
        if let binds = binds {
            for bind in binds {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
            }
            
            if let bindPtr = bindPtr {
                bindPtr.deallocate(capacity: binds.count)
            }
        }
        
        return Error.database(Connection.getError(from: preparedStatement.statement!))
    }
    
    
    /// All results of an execution, as an array of dictionaries.
    ///
    /// - Returns: An array `[[String : Any]]` of rows returned by the database.
    func rows() -> MySQL.Result {
        var rows = [[String : Any?]]()
        while true {
            if let result = fetchNext() {
                var row = [String : Any?]()
                guard result.count == fieldNames.count else {
                    print("ERROR: Fetched row contains \(result.count) fields, but we only have \(fieldNames.count) field names.")
                    continue
                }
                for pair in zip(fieldNames, result) {
                    row[pair.0] = pair.1
                }
                rows.append(row)
            } else {
                break
            }
        }
        return rows
    }
    
    
    /// Fetch the next row of the query result. This function is blocking.
    ///
    /// - Returns: An array of values of type Any? representing the next row from the query result.
    private func fetchNext() -> [Any?]? {
        guard hasMoreRows else {
            return nil
        }
        
        if let row = buildRow() {
            return row
        } else {
            hasMoreRows = false
            close()
            return nil
        }
    }
    
    private static func getOutputBind(_ field: MYSQL_FIELD) -> MYSQL_BIND {
        let size = getSize(field: field)
        
        var bind = MYSQL_BIND()
        bind.buffer_type = field.type
        bind.buffer_length = UInt(size)
        bind.is_unsigned = 0
        
        bind.buffer = UnsafeMutableRawPointer.allocate(bytes: size, alignedTo: 1)
        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.error = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        
        return bind
    }
    
    private static func getSize(field: MYSQL_FIELD) -> Int {
        switch field.type {
        case MYSQL_TYPE_TINY:
            return MemoryLayout<Int8>.size
        case MYSQL_TYPE_SHORT:
            return MemoryLayout<Int16>.size
        case MYSQL_TYPE_INT24,
             MYSQL_TYPE_LONG:
            return MemoryLayout<Int32>.size
        case MYSQL_TYPE_LONGLONG:
            return MemoryLayout<Int64>.size
        case MYSQL_TYPE_FLOAT:
            return MemoryLayout<Float>.size
        case MYSQL_TYPE_DOUBLE:
            return MemoryLayout<Double>.size
        case MYSQL_TYPE_TIME,
             MYSQL_TYPE_DATE,
             MYSQL_TYPE_DATETIME,
             MYSQL_TYPE_TIMESTAMP:
            return MemoryLayout<MYSQL_TIME>.size
        default:
            return Int(field.length)
        }
    }
    
    private func buildRow() -> [Any?]? {
        let fetchStatus = mysql_stmt_fetch(preparedStatement.statement)
        if fetchStatus == MYSQL_NO_DATA {
            return nil
        }
        
        if fetchStatus == 1 {
            print("Error fetching row: \(Connection.getError(from: preparedStatement.statement!))")
            return nil
        }
        
        var row = [Any?]()
        for bind in binds {
            guard let buffer = bind.buffer else {
                // Note: This is an error, but we append nil and continue
                print("Error reading data: bind buffer not set.")
                row.append(nil)
                continue
            }
            
            guard bind.is_null.pointee == 0 else {
                row.append(nil)
                continue
            }
            
            let type = bind.buffer_type
            switch type {
            case MYSQL_TYPE_TINY:
                row.append(buffer.load(as: Int8.self))
            case MYSQL_TYPE_SHORT:
                row.append(buffer.load(as: Int16.self))
            case MYSQL_TYPE_INT24,
                 MYSQL_TYPE_LONG:
                row.append(buffer.load(as: Int32.self))
            case MYSQL_TYPE_LONGLONG:
                row.append(buffer.load(as: Int64.self))
            case MYSQL_TYPE_FLOAT:
                row.append(buffer.load(as: Float.self))
            case MYSQL_TYPE_DOUBLE:
                row.append(buffer.load(as: Double.self))
            case MYSQL_TYPE_NEWDECIMAL,
                 MYSQL_TYPE_STRING,
                 MYSQL_TYPE_VAR_STRING:
                row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: .utf8, freeWhenDone: false))
            case MYSQL_TYPE_TINY_BLOB,
                 MYSQL_TYPE_BLOB,
                 MYSQL_TYPE_MEDIUM_BLOB,
                 MYSQL_TYPE_LONG_BLOB,
                 MYSQL_TYPE_BIT:
                row.append(Data(bytes: buffer, count: getLength(bind)))
            case MYSQL_TYPE_TIME:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(pad(time.hour)):\(pad(time.minute)):\(pad(time.second))")
            case MYSQL_TYPE_DATE:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(time.year)-\(pad(time.month))-\(pad(time.day))")
            case MYSQL_TYPE_DATETIME,
                 MYSQL_TYPE_TIMESTAMP:
                let time = buffer.load(as: MYSQL_TIME.self)
                let formattedDate = "\(time.year)-\(time.month)-\(time.day) \(time.hour):\(time.minute):\(time.second)"
                let dateFormatter = DateFormatter()
                dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
                row.append(dateFormatter.date(from: formattedDate))
            default:
                print("Using string for unhandled enum_field_type: \(type.rawValue)")
                row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: .utf8, freeWhenDone: false))
            }
        }
        
        return row
    }
    
    
    private func close() {
        if let bindPtr = bindPtr {
            self.bindPtr = nil
            
            for bind in binds {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
            }
            bindPtr.deallocate(capacity: binds.count)
            
            preparedStatement.release()
        }
    }
    
    
    private func getLength(_ bind: MYSQL_BIND) -> Int {
        return Int(bind.length.pointee > bind.buffer_length ? bind.buffer_length : bind.length.pointee)
    }
    
    private func pad(_ uInt: UInt32) -> String {
        return String(format: "%02u", uInt)
    }
    
}




