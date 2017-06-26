//
//  Connection.swift
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


/// A single MySQL connection.
/// An instance of `Connection` is NOT thread safe.
class Connection {
    
    /// Errors that may be thrown by a connection.
    enum Error: Swift.Error, LocalizedError {
        case connect(String)
        case execute(String)
        
        var errorDescription: String? {
            switch self {
            case .connect(let msg):
                return msg
            case .execute(let msg):
                return msg
            }
        }
    }
    
    
    /// This ensures that `mysql_server_init` gets called exactly once, ever.
    private static let initOnce: () = {
        // This call is not thread-safe
        mysql_server_init(0, nil, nil)
    }()
    
    /// Connection settings.
    private let settings: MySQL.Settings
    
    /// Reference to MySQL database.
    private var mysql: UnsafeMutablePointer<MYSQL>?
    
    /// State variable.
    private var inTransaction = false
    
    /// State variable identifiying whether the connection is currently active.
    public var isConnected: Bool {
        return self.mysql != nil
    }
    
    
    /// Initialize an instance of Connection.
    ///
    /// - Parameter host: host name or IP address of server to connect to, defaults to localhost
    /// - Parameter user: MySQL login ID, defaults to current user
    /// - Parameter password: password for `user`, defaults to no password
    /// - Parameter database: default database to use if specified
    /// - Parameter port: port number for the TCP/IP connection if using a non-standard port
    /// - Parameter unixSocket: unix domain socket or named pipe to use for connecting to server instead of TCP/IP
    /// - Parameter clientFlag: MySQL client options
    /// - Parameter characterSet: MySQL character set to use for the connection
    init?(settings: MySQL.Settings) {
        // Ensure that mysql_server_init gets called exactly once, ever.
        Connection.initOnce
        
        // Store connection settings
        self.settings = settings
        
        // Connect to server
        do {
            try self.connect()
        } catch {
            return nil
        }
        
        if mysql == nil {
            return nil
        }
    }
    
    deinit {
        close()
    }
    
    
    /// Establish a connection with the database.
    private func connect() throws {
        let mysql: UnsafeMutablePointer<MYSQL> = self.mysql ?? mysql_init(nil)
        
        if mysql_real_connect(mysql, settings.host, settings.user, settings.password,
                              settings.database, settings.port, settings.unixSocket, settings.clientFlag) != nil ||
            mysql_errno(mysql) == UInt32(CR_ALREADY_CONNECTED) {
            
            if mysql_set_character_set(mysql, settings.characterSet) != 0 {
                let defaultCharSet = String(cString: mysql_character_set_name(mysql))
                print("WARNING: Invalid characterSet: \(settings.characterSet), using: \(defaultCharSet)")
            }
            
            // Success
            self.mysql = mysql
        } else {
            self.mysql = nil
            let error = Connection.getError(from: mysql)
            mysql_thread_end() // Must be called for each mysql_init() call
            throw Error.connect(error)
        }
    }
    
    
    /// Close the connection to the database.
    func close() {
        if let mysql = self.mysql {
            self.mysql = nil
            mysql_close(mysql)
            mysql_thread_end() // Must be called for each mysql_init() call
        }
    }
    
    
    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String) throws -> MySQL.Result {
        let preparedStatement = try PreparedStatement(raw, mysql: mysql)
        let result = try preparedStatement.execute()
        preparedStatement.release()
        return result
    }
    
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?]) throws -> MySQL.Result {
        let preparedStatement = try PreparedStatement(raw, mysql: mysql)
        let result = try preparedStatement.execute(parameters: parameters)
        preparedStatement.release()
        return result
    }
    
    
    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
//    public func prepareStatement(_ raw: String) throws -> PreparedStatement  {
//        return try PreparedStatement(raw, mysql: mysql)
//    }
    
    
    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
//    public func release(preparedStatement: PreparedStatement) {
//        preparedStatement.release()
//    }
    
    
    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    func execute(preparedStatement: PreparedStatement) throws -> MySQL.Result  {
        return try preparedStatement.execute()
    }
    
    
    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    func execute(preparedStatement: PreparedStatement, parameters: [Any?]) throws -> MySQL.Result {
        return try preparedStatement.execute(parameters: parameters)
    }
    
    
    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    func startTransaction() throws -> MySQL.Result {
        return try executeTransaction(command: "START TRANSACTION", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to start the transaction")
    }
    
    
    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    func commit() throws -> MySQL.Result {
        return try executeTransaction(command: "COMMIT", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to commit the transaction")
    }
    
    
    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    func rollback() throws -> MySQL.Result {
        return try executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction")
    }
    
    
    func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String) throws -> MySQL.Result {
        
        guard let mysql = self.mysql else {
            throw Error.execute("Not connected, be sure to call connect() first.")
        }
        
        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists." : "No transaction exists."
            throw Error.execute(error)
        }
        
        if mysql_query(mysql, command) == 0 {
            if changeTransactionState {
                self.inTransaction = !self.inTransaction
            }
            
            
            // TODO: Return success message?
            return [[:]]
//            onCompletion(.successNoData)
            
        } else {
            throw Error.execute("\(errorMessage): \(Connection.getError(from: mysql))")
        }
    }
    
    
    /// Extracts an error message from a MySQL connection.
    ///
    /// - Parameter connection: A MySQL connection reference.
    /// - Returns: The error as a String.
    static func getError(from connection: UnsafeMutablePointer<MYSQL>) -> String {
        return "Error \(mysql_errno(connection)): " + String(cString: mysql_error(connection))
    }
    
    
    /// Extracts an error message from a MySQL prepared statement.
    ///
    /// - Parameter statement: A MySQL prepared statement reference.
    /// - Returns: The error as a String.
    static func getError(from statement: UnsafeMutablePointer<MYSQL_STMT>) -> String {
        return "Error \(mysql_stmt_errno(statement)): " + String(cString: mysql_stmt_error(statement))
    }
    
}

