//
//  PreparedStatement.swift
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


/// MySQL implementation for prepared statements.
public class PreparedStatement {
    
    enum Error: Swift.Error, LocalizedError {
        case initialize(String)
        case execute(String)
        
        var errorDescription: String? {
            switch self {
            case .execute(let msg):
                return msg
            case .initialize(let msg):
                return msg
            }
        }
    }
    
    /// Reference to native MySQL statement.
    private(set) var statement: UnsafeMutablePointer<MYSQL_STMT>?
    
    private var binds = [MYSQL_BIND]()
    private var bindsCapacity = 0
    private var bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil
    
    /// Static date formatter for converting MySQL fetch results to Swift types.
    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()
    
    
    /// Initializes a prepared statement.
    ///
    /// - Parameters:
    ///   - stmt: The statement to execute.
    ///   - mysql: A reference to a MySQL connection.
    /// - Throws: `PreparedStatement.Error` if an error is encountered.
    init(_ stmt: String, mysql: UnsafeMutablePointer<MYSQL>?) throws {
        // Make sure database connection has already been established
        guard let mysql = mysql else {
            throw Error.initialize("MySQL not connected. Call connect() before execute().")
        }
        
        // Initialize MySQL statement
        guard let statement = mysql_stmt_init(mysql) else {
            throw Error.initialize(Connection.getError(from: mysql))
        }
        
        // Prepare statement
        guard mysql_stmt_prepare(statement, stmt, UInt(stmt.utf8.count)) == 0 else {
            defer {
                mysql_stmt_close(statement)
            }
            throw Error.initialize(Connection.getError(from: statement))
        }
        
        self.statement = statement
    }
    
    
    /// Ensure that statement and binds become deallocated.
    deinit {
        release()
    }
    
    
    /// Execute the statement, optionally with parameters.
    ///
    /// - Parameter parameters: An array of parameters to use for execution, if any.
    /// - Returns: The result of the execution.
    /// - Throws: `PreparedStatement.Error` if an error occurs.
    func execute(parameters: [Any?]? = nil) throws -> MySQL.Result {
        guard let statement = self.statement else {
            throw Error.execute("The prepared statement has already been released.")
        }
        
        if let parameters = parameters {
            if let bindPtr = bindPtr {
                guard bindsCapacity == parameters.count else {
                    throw Error.execute("Each call to execute() must pass the same number of parameters.")
                }
            } else { // true only for the first time execute() is called for this PreparedStatement
                bindsCapacity = parameters.count
                bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: bindsCapacity)
            }
            
            do {
                try allocateBinds(parameters: parameters)
            } catch {
                // Catch error so we can close the statement before throwing
                self.statement = nil
                mysql_stmt_close(statement)
                
                throw error
            }
        }
        
        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            // Non-query statement (insert, update, delete)
            
            guard mysql_stmt_execute(statement) == 0 else {
                guard let statement = self.statement else {
                    throw Error.execute("Statement is nil after execution.")
                }
                let error = Connection.getError(from: statement)
                self.statement = nil
                mysql_stmt_close(statement)
                throw Error.execute(error)
            }
            
            // TODO: Potentially return number of affected rows here
//            let affectedRows = mysql_stmt_affected_rows(statement) as UInt64
            return [[:]]
        }
        
        defer {
            mysql_free_result(resultMetadata)
        }

        let resultFetcher = try ResultFetcher(preparedStatement: self, resultMetadata: resultMetadata)
        return resultFetcher.rows()
    }
    
    
    /// Deallocate statement and binds.
    func release() {
        deallocateBinds()
        
        if let statement = self.statement {
            self.statement = nil
            mysql_stmt_close(statement)
        }
    }
    
    
    private func allocateBinds(parameters: [Any?]) throws {
        if binds.isEmpty { // first parameter set, create new bind and bind it to the parameter
            for (index, parameter) in parameters.enumerated() {
                var bind = MYSQL_BIND()
                setBind(&bind, parameter)
                binds.append(bind)
                bindPtr![index] = bind
            }
        } else { // bind was previously created, re-initialize value
            for (index, parameter) in parameters.enumerated() {
                var bind = binds[index]
                setBind(&bind, parameter)
                binds[index] = bind
                bindPtr![index] = bind
            }
        }
        
        guard mysql_stmt_bind_param(statement, bindPtr) == 0 else {
            throw Error.execute(Connection.getError(from: statement!)) // THis is guaranteed to be safe
        }
    }
    
    private func deallocateBinds() {
        guard let bindPtr = self.bindPtr else {
            return
        }
        
        self.bindPtr = nil
        
        for bind in binds {
            if bind.buffer != nil {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
            }
            if bind.length != nil {
                bind.length.deallocate(capacity: 1)
            }
            if bind.is_null != nil {
                bind.is_null.deallocate(capacity: 1)
            }
        }
        bindPtr.deallocate(capacity: bindsCapacity)
        binds.removeAll()
    }
    
    private func setBind(_ bind: inout MYSQL_BIND, _ parameter: Any?) {
        if bind.is_null == nil {
            bind.is_null = UnsafeMutablePointer<Int8>.allocate(capacity: 1)
        }
        
        guard let parameter = parameter else {
            bind.buffer_type = MYSQL_TYPE_NULL
            bind.is_null.initialize(to: 1)
            return
        }
        
        bind.buffer_type = getType(parameter: parameter)
        bind.is_null.initialize(to: 0)
        bind.is_unsigned = 0
        
        switch parameter {
        case let string as String:
            initialize(string: string, &bind)
        case let date as Date:
            // Note: Here we assume this is DateTime type - does not handle Date or Time types
            let formattedDate = PreparedStatement.dateFormatter.string(from: date)
            initialize(string: formattedDate, &bind)
        case let byteArray as [UInt8]:
            let typedBuffer = allocate(type: UInt8.self, capacity: byteArray.count, bind: &bind)
            #if swift(>=3.1)
                let _ = UnsafeMutableBufferPointer(start: typedBuffer, count: byteArray.count).initialize(from: byteArray)
            #else
                typedBuffer.initialize(from: byteArray)
            #endif
        case let data as Data:
            let typedBuffer = allocate(type: UInt8.self, capacity: data.count, bind: &bind)
            data.copyBytes(to: typedBuffer, count: data.count)
        case let dateTime as MYSQL_TIME:
            initialize(dateTime, &bind)
        case let float as Float:
            initialize(float, &bind)
        case let double as Double:
            initialize(double, &bind)
        case let bool as Bool:
            initialize(bool, &bind)
        case let int as Int:
            initialize(int, &bind)
        case let int as Int8:
            initialize(int, &bind)
        case let int as Int16:
            initialize(int, &bind)
        case let int as Int32:
            initialize(int, &bind)
        case let int as Int64:
            initialize(int, &bind)
        case let uint as UInt:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt8:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt16:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt32:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt64:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let unicodeScalar as UnicodeScalar:
            initialize(unicodeScalar, &bind)
            bind.is_unsigned = 1
        default:
            print("WARNING: Unhandled parameter \(parameter) (type: \(type(of: parameter))). Will attempt to convert it to a String")
            initialize(string: String(describing: parameter), &bind)
        }
    }
    
    private func allocate<T>(type: T.Type, capacity: Int, bind: inout MYSQL_BIND) -> UnsafeMutablePointer<T> {
        
        let length = UInt(capacity * MemoryLayout<T>.size)
        if bind.length == nil {
            bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        }
        bind.length.initialize(to: length)
        
        let typedBuffer: UnsafeMutablePointer<T>
        if let buffer = bind.buffer, bind.buffer_length >= length {
            typedBuffer = buffer.assumingMemoryBound(to: type)
        } else {
            if bind.buffer != nil {
                // deallocate existing smaller buffer
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
            }
            
            typedBuffer = UnsafeMutablePointer<T>.allocate(capacity: capacity)
            bind.buffer = UnsafeMutableRawPointer(typedBuffer)
            bind.buffer_length = length
        }
        
        return typedBuffer
    }
    
    private func initialize<T>(_ parameter: T, _ bind: inout MYSQL_BIND) {
        let typedBuffer = allocate(type: type(of: parameter), capacity: 1, bind: &bind)
        typedBuffer.initialize(to: parameter)
    }
    
    private func initialize(string: String, _ bind: inout MYSQL_BIND) {
        let utf8 = string.utf8
        let typedBuffer = allocate(type: UInt8.self, capacity: utf8.count, bind: &bind)
        #if swift(>=3.1)
            let _ = UnsafeMutableBufferPointer(start: typedBuffer, count: utf8.count).initialize(from: utf8)
        #else
            typedBuffer.initialize(from: utf8)
        #endif
    }
    
    private func getType(parameter: Any) -> enum_field_types {
        switch parameter {
        case is String,
             is Date:
            return MYSQL_TYPE_STRING
        case is Data,
             is [UInt8]:
            return MYSQL_TYPE_BLOB
        case is Int8,
             is UInt8,
             is Bool:
            return MYSQL_TYPE_TINY
        case is Int16,
             is UInt16:
            return MYSQL_TYPE_SHORT
        case is Int32,
             is UInt32,
             is UnicodeScalar:
            return MYSQL_TYPE_LONG
        case is Int,
             is UInt,
             is Int64,
             is UInt64:
            return MYSQL_TYPE_LONGLONG
        case is Float:
            return MYSQL_TYPE_FLOAT
        case is Double:
            return MYSQL_TYPE_DOUBLE
        case is MYSQL_TIME:
            return MYSQL_TYPE_DATETIME
        default:
            return MYSQL_TYPE_STRING
        }
    }
}



