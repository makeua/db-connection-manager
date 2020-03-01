package org.task.dbcm.connectionmanager.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
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

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

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
    public void close() {
        LOG.trace("ConnectionPoolImpl::close() started");
        for (PooledConnection connection : activePooledConnections) {
            closePooledConnection(connection);
        }
        for (BlockingDeque<PooledConnection> connections : pooledConnections.values()) {
            for (PooledConnection connection : connections) {
                closePooledConnection(connection);
            }
        }
        LOG.trace("ConnectionPoolImpl::close() finished");
    }

    private BlockingDeque<PooledConnection> getPooledConnectionDeque(PooledConnectionKey pooledConnectionKey) {
        return pooledConnections.computeIfAbsent(pooledConnectionKey, k -> new LinkedBlockingDeque<>());
    }

    private PooledConnection getConnection(PooledConnectionKey pooledConnectionKey) throws SQLException {
        synchronized (monitor) {
            try {
                LOG.trace("ConnectionPoolImpl::getConnection(PooledConnection pooledConnection) started");

                BlockingDeque<PooledConnection> pooledConnectionsDeque = getPooledConnectionDeque(pooledConnectionKey);
                LOG.debug("Amount of current connections: {}", connectionNumber.get());
                if (connectionNumber.get() < maxPoolSize && pooledConnectionsDeque.isEmpty()) {
                    pooledConnectionsDeque.add(createNewConnection(pooledConnectionKey));
                }

                LOG.debug("Getting connection from the queue...");
                PooledConnection pooledConnection;
                while ((pooledConnection = pooledConnectionsDeque.poll()) == null) {
                    // waiting for an available connection
                }
                LOG.debug("Connection got from the queue");

                if (closeIfNeeded(pooledConnection)) {
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

            Connection connection = pooledConnectionKey.isCredentials()
                    ? credentialConnectionSupplier.get(pooledConnectionKey.getUsername(), pooledConnectionKey.getPassword())
                    : connectionSupplier.get();

            connectionNumber.incrementAndGet();
            return new PooledConnection(
                    pooledConnectionKey,
                    System.currentTimeMillis(),
                    this,
                    connection);
        } finally {
            LOG.trace("ConnectionPoolImpl::createNewConnection(PooledConnection pooledConnection) finished");
        }
    }

    private boolean closeIfNeeded(PooledConnection pooledConnection) {
        long currentTime = System.currentTimeMillis();
        long aliveTime = currentTime - pooledConnection.getCreationTime();
        LOG.debug("Checking connection alive time, created at [{}], current [{}], alive time [{} s], TTL [{} s]",
                FORMAT.format(new Date(pooledConnection.getCreationTime())),
                FORMAT.format(new Date(currentTime)),
                aliveTime / 1000.0,
                connectionTTL / 1000.0);
        if ((currentTime - pooledConnection.getCreationTime()) > connectionTTL) {
            LOG.debug("Connection time to live is over, closing and creating new");
            closePooledConnection(pooledConnection);
            return true;
        }
        return false;
    }

    private void closePooledConnection(PooledConnection pooledConnection) {
        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) started");

        connectionNumber.decrementAndGet();

        try {
            LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) connection close started");
            pooledConnection.getUnderlyingConnection().close();
        } catch (SQLException e) {
            LOG.debug("Failed to close pooled connection");
        } finally {
            LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) connection close finished");
        }

        LOG.trace("ConnectionPoolImpl::closePooledConnection(PooledConnection pooledConnection) finished");
    }

    void returnConnection(PooledConnection pooledConnection) {
        LOG.trace("ConnectionPoolImpl::returnConnection(PooledConnection pooledConnection) started");

        getPooledConnectionDeque(pooledConnection.getPooledConnectionKey()).add(pooledConnection);
        activePooledConnections.remove(pooledConnection);

        LOG.trace("ConnectionPoolImpl::returnConnection(PooledConnection pooledConnection) finished");
    }
}