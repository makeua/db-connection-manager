package org.task.dbcm.connectionmanager.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection pool which stores created connections to avoid unnecessary creation.
 */
final class ConnectionPoolImpl implements ConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPoolImpl.class);

    private final ConnectionSupplier connectionSupplier;
    private final CredentialConnectionSupplier credentialConnectionSupplier;

    private final long connectionTTL;
    private final int maxPoolSize;

    private final Object monitor = new Object();

    private ConcurrentMap<PooledConnectionKey, BlockingDeque<PooledConnection>> pooledConnections;
    private AtomicInteger connectionNumber;
    private Set<PooledConnection> activePooledConnections;

    ConnectionPoolImpl(ConnectionSupplier connectionSupplier,
                       CredentialConnectionSupplier credentialConnectionSupplier,
                       long connectionTTL,
                       int maxPoolSize) {
        this.connectionSupplier = Objects.requireNonNull(connectionSupplier, "ConnectionSupplier cannot be null");
        this.credentialConnectionSupplier = Objects.requireNonNull(credentialConnectionSupplier, "CredentialConnectionSupplier cannot be null");
        this.connectionTTL = connectionTTL;
        this.maxPoolSize = maxPoolSize;

        this.pooledConnections = new ConcurrentHashMap<>();
        this.connectionNumber = new AtomicInteger(0);
        this.activePooledConnections = Collections.synchronizedSet(new HashSet<>());
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            LOG.trace("ConnectionPoolImpl::getConnection() started");
            return getConnection(new PooledConnectionKey());
        } finally {
            LOG.trace("ConnectionPoolImpl::getConnection() finished");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        try {
            LOG.trace("ConnectionPoolImpl::getConnection(String username, String password) started");
            return getConnection(new PooledConnectionKey(username, password));
        } finally {
            LOG.trace("ConnectionPoolImpl::getConnection(String username, String password) finished");
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            LOG.trace("ConnectionPoolImpl::close() started");
            for (PooledConnection connection : activePooledConnections) {
                closePooledConnection(connection);
            }
            for (BlockingDeque<PooledConnection> connections : pooledConnections.values()) {
                for (PooledConnection connection : connections) {
                    closePooledConnection(connection);
                }
            }
        } finally {
            LOG.trace("ConnectionPoolImpl::close() finished");
        }
    }

    private BlockingDeque<PooledConnection> getPooledConnectionDeque(PooledConnectionKey pooledConnectionKey) {
        return pooledConnections.computeIfAbsent(pooledConnectionKey, k -> new LinkedBlockingDeque<>());
    }

    private PooledConnection getConnection(PooledConnectionKey pooledConnectionKey) throws SQLException {
        synchronized (monitor) {
            try {
                LOG.trace("ConnectionPoolImpl::getConnection(PooledConnection pooledConnection) started");

                BlockingDeque<PooledConnection> pooledConnectionsDeque = getPooledConnectionDeque(pooledConnectionKey);
                LOG.trace("Amount of current connections: {}", connectionNumber.get());
                if (connectionNumber.get() < maxPoolSize && pooledConnectionsDeque.isEmpty()) {
                    pooledConnectionsDeque.add(createNewConnection(pooledConnectionKey));
                }

                LOG.trace("Getting connection from the queue...");
                PooledConnection pooledConnection;
                while ((pooledConnection = pooledConnectionsDeque.poll()) == null) {
                    // waiting for an available connection
                }
                LOG.trace("Connection got from the queue");

                // if it's time for the connection to be closed and renewed
                LOG.trace("Checking connection alive time");
                if ((System.currentTimeMillis() - pooledConnection.getCreationTime()) > connectionTTL) {
                    LOG.trace("Connection TTL is up, closing and creating new");
                    closePooledConnection(pooledConnection);
                    pooledConnection = createNewConnection(pooledConnectionKey);
                }
                activePooledConnections.add(pooledConnection);
                return pooledConnection;
            } finally {
                LOG.trace("ConnectionPoolImpl::getConnection(PooledConnection pooledConnection) finished");
            }
        }
    }

    private PooledConnection createNewConnection(PooledConnectionKey pooledConnectionKey) throws SQLException {
        try {
            LOG.trace("ConnectionPoolImpl::createNewConnection(PooledConnection pooledConnection) started");

            connectionNumber.incrementAndGet();
            return new PooledConnection(
                    pooledConnectionKey,
                    System.currentTimeMillis(),
                    this,
                    pooledConnectionKey.isCredentials()
                            ? credentialConnectionSupplier.get(pooledConnectionKey.getUsername(), pooledConnectionKey.getPassword())
                            : connectionSupplier.get());
        } finally {
            LOG.trace("ConnectionPoolImpl::createNewConnection(PooledConnection pooledConnection) finished");
        }
    }

    private void closePooledConnection(PooledConnection pooledConnection) throws SQLException {
        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) started");

        connectionNumber.decrementAndGet();

        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) connection close started");
        pooledConnection.getUnderlyingConnection().close();
        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) connection close finished");

        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) finished");
    }

    void returnConnection(PooledConnection pooledConnection) {
        LOG.trace("ConnectionPoolImpl::returnConnection(PooledConnection pooledConnection) started");

        getPooledConnectionDeque(pooledConnection.getPooledConnectionKey()).add(pooledConnection);
        activePooledConnections.remove(pooledConnection);

        LOG.trace("ConnectionPoolImpl::returnConnection(PooledConnection pooledConnection) finished");
    }
}