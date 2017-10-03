//
//  ConnectionPool.swift
//  MySQL
//
//  Created by Collin Hundley on 6/24/17.
//

import Foundation
import Dispatch
#if os(Linux)
    import Glibc
#else
    import Darwin
#endif


/// A pool of MySQL connections. FIFO.
class ConnectionPool {
    
    /// The set of connections maintained by the pool.
    private var pool = [Connection]()
    
    /// The connection settings.
    private let connectionSettings: MySQL.Settings
    
    /// Semaphore to ensure thread safety when accessing the pool.
    private var poolLock = DispatchSemaphore(value: 1)
    
    /// The maximum number of connections to maintain in the pool.
    private let limit: Int
    
    /// The initial size of this pool.
    private var capacity: Int
    
    /// A semaphore to enable take() to block when the pool is empty.
    private var semaphore: DispatchSemaphore
    
    /// Timeout (in seconds) to wait before returning nil from take().
    private let timeout: Int
    
    
    /// Initializes a new connection pool.
    ///
    /// - Parameters:
    ///   - initialCapacity: The initial number of connections in the pool.
    ///   - maxCapacity: The maximum size that the pool may grow.
    ///   - settings: MySQL connection settings.
    /// - Throws: `MySQL.Error` if the connection fails.
    init(initialCapacity: Int, maxCapacity: Int, settings: MySQL.Settings) throws {
        connectionSettings = settings
        capacity = initialCapacity < 1 ? 1 : initialCapacity
        limit = maxCapacity < 1 ? 1 : maxCapacity
        timeout = 10
        semaphore = DispatchSemaphore(value: capacity)
        for _ in 0 ..< capacity {
            // Populate pool to initial capacity
            if let connection = Connection(settings: settings) {
                pool.append(connection)
            }
            else {
                throw MySQL.Error.connect("Failed to connect to MySQL host.")
            }
        }
    }
    
    
    /// Ensure that all connections are released when pool is released.
    deinit {
        disconnect()
    }
    
    
    /// Retrieves a connection from the pool.
    /// Each call should be balanced with a call to `return()`.
    func getConnection() -> Connection? {
        return take()
    }
    
    
    /// Returns a connection to the pool.
    func `return`(_ connection: Connection) {
        give(connection)
    }
    
    
    /// Removes a connection from the pool.
    /// This function will block until a connection becomes available in the pool,
    /// or until the timeout is reached.
    /// Each call to `take()` should be balanced with a call to `give()`.
    private func take() -> Connection? {
        var item: Connection!
        // Indicate that we are going to take an item from the pool. The semaphore will
        // block if there are currently no items to take, until one is returned via give()
        let result = semaphore.wait(timeout: (timeout == 0) ? .distantFuture : .now() + DispatchTimeInterval.seconds(timeout))
        if result == DispatchTimeoutResult.timedOut {
            return nil
        }
        
        // We have permission to take an item - do so in a thread-safe way
        lockPoolLock()
        if (pool.count < 1) {
            unlockPoolLock()
            return nil
        }
        item = pool[0]
        pool.removeFirst()
        
        // Verify that this connection is still alive (i.e. hasn't timed out)
        // Note: The MYSQL_OPT_RECONNECT flag should already take care of reconnects,
        // but this is an extra backup just in case it fails
        if !item.isConnected {
            // Connection timed out; create a new one
            item = Connection(settings: connectionSettings)
        }
        
        // If we took the last item, we can choose to grow the pool
        if (pool.count == 0 && capacity < limit) {
            capacity += 1
            if let connection = Connection(settings: connectionSettings) {
                pool.append(connection)
                semaphore.signal()
            }
        }
        unlockPoolLock()
        return item
    }
    
    
    // Give an item back to the pool. Whilst this item would normally be one that was earlier
    // take()n from the pool, a new item could be added to the pool via this method.
    private func give(_ connection: Connection) {
        lockPoolLock()
        pool.append(connection)
        // Signal that an item is now available
        semaphore.signal()
        unlockPoolLock()
    }
    
    
    /// Releases all connections in the pool, and then empties the pool.
    func disconnect() {
        // Thread safe
        lockPoolLock()
        // Release all connections
        for connection in pool {
            connection.close()
        }
        // Remove all items from the array
        pool.removeAll()
        unlockPoolLock()
    }
    
    
    private func lockPoolLock() {
        _ = poolLock.wait(timeout: DispatchTime.distantFuture)
    }
    
    
    private func unlockPoolLock() {
        poolLock.signal()
    }
    
}
