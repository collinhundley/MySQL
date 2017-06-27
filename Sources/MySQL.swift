//
// MySQL.swift
//

import Foundation


public class MySQL {
    
    /// The result of a statement execution.
    public typealias Result = [[String : Any?]]
    
    /// All errors that may be thrown by `MySQL`.
    public enum Error: Swift.Error, LocalizedError {
        /// General connection issues.
        case connect(String)
        /// An error executing a statement.
        case execute(String)
        
        public var errorDescription: String? {
            switch self {
            case .connect(let msg):
                return msg
            case .execute(let msg):
                return msg
            }
        }
    }
    
    
    /// A pool of connections maintained by the database.
    fileprivate let connectionPool: ConnectionPool
    
    
    /// Creates a MySQL instance which maintains a pool of connections.
    ///
    /// - Parameters:
    ///   - settings: The MySQL connection settings.
    ///   - initialPoolSize: The initial number of connections in the pool.
    ///   - maxPoolSize: The maximum size to which the connection pool may grow.
    /// - Throws: `MySQL.Error` if an error is encountered connecting to the database.
    public init(settings: MySQL.Settings, initialPoolSize: Int, maxPoolSize: Int) throws {
        self.connectionPool = try ConnectionPool(initialCapacity: initialPoolSize,
                                                 maxCapacity: maxPoolSize,
                                                 settings: settings)
    }
    
}


// MARK: Settings

public extension MySQL {
    
    /// Connection settings.
    public struct Settings {
        let host: String
        let user: String
        let password: String
        let database: String
        let port: UInt32
        let unixSocket: String?
        let clientFlag: UInt
        let characterSet: String
        
        public init(host: String, user: String?, password: String?,
                    database: String?, port: Int, unixSocket: String? = nil,
                    clientFlag: UInt? = nil, characterSet: String? = nil) {
            self.host = host
            self.user = user ?? ""
            self.password = password ?? ""
            self.database = database ?? ""
            self.port = UInt32(port)
            self.unixSocket = unixSocket
            self.clientFlag = clientFlag ?? 0
            self.characterSet = characterSet ?? "utf8"
        }
    }
    
}


// MARK: Execute

public extension MySQL {
    
    /// Execute a statement without parameters.
    ///
    /// - Parameter statement: THe statement to execute.
    /// - Returns: The result of the statement's execution, if any.
    /// - Throws: `MySQL.Error` if an error is encountered.
    @discardableResult
    public func execute(_ statement: String) throws -> Result {
        // Extract a connection from the pool
        guard let connection = connectionPool.getConnection() else {
            throw Error.connect("Timeout occurred while retrieving a connection from the pool.")
        }
        
        // Execute statement
        // We ensure that the connection always gets returned to the pool
        do {
            let result = try connection.execute(statement)
            connectionPool.return(connection)
            return result
        } catch {
            connectionPool.return(connection)
            throw Error.execute(error.localizedDescription)
        }
    }
    
    
    /// Execute a statement with parameters.
    ///
    /// - Parameters:
    ///   - statement: The statement to execute.
    ///   - parameters: The result of the statement's execution, if any.
    /// - Returns: The result of the statement's execution, if any.
    /// - Throws: `MySQL.Error` if an error is encountered.
    @discardableResult
    public func execute(_ statement: String, parameters: [Any?]) throws -> Result {
        // Extract a connection from the pool
        guard let connection = connectionPool.getConnection() else {
            throw Error.connect("Timeout occurred while retrieving a connection from the pool.")
        }
        
        // Execute statement
        // We ensure that the connection always gets returned to the pool
        do {
            let result = try connection.execute(statement, parameters: parameters)
            connectionPool.return(connection)
            return result
        } catch {
            connectionPool.return(connection)
            throw Error.execute(error.localizedDescription)
        }
    }
    
}


