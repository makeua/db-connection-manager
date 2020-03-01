package org.task.dbcm.connectionmanager.datasource;

import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Implementation of the pooled data source. Delegates calls of all methods to the underlying data source
 * except getConnection methods. Calls to getConnection methods retrieves a connection from the pool.
 * Uses given ConnectionPool to store created connections and return them by request. If the connection pool is empty
 * and no connections were created a connection is created using the call to the underlying data source and adding created connection to the pool.
 * <p>
 * Calls to getConnection may block the calling thread because there might be no available connection to use.
 */
final class PooledDataSourceImpl implements PooledDataSource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PooledDataSourceImpl.class);

    private final CloseableDataSource underlyingDataSource;
    private final ConnectionPool connectionPool;

    public PooledDataSourceImpl(CloseableDataSource underlyingDataSource, ConnectionPool connectionPool) {
        this.underlyingDataSource = Objects.requireNonNull(underlyingDataSource, "Underlying data source cannot be null");
        this.connectionPool = Objects.requireNonNull(connectionPool, "ConnectionPool cannot be null");
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::getConnection() started");
            return connectionPool.getConnection();
        } finally {
            LOG.trace("PooledDataSourceImpl::getConnection() finished");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::getConnection(String username, String password) started");
            return connectionPool.getConnection(username, password);
        } finally {
            LOG.trace("PooledDataSourceImpl::getConnection(String username, String password) finished");
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::getLogWriter() started");
            return underlyingDataSource.getLogWriter();
        } finally {
            LOG.trace("PooledDataSourceImpl::getLogWriter() finished");
        }
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::setLogWriter(PrintWriter out) started");
            underlyingDataSource.setLogWriter(out);
        } finally {
            LOG.trace("PooledDataSourceImpl::setLogWriter(PrintWriter out) finished");
        }
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::setLoginTimeout(int seconds) started");
            underlyingDataSource.setLoginTimeout(seconds);
        } finally {
            LOG.trace("PooledDataSourceImpl::setLoginTimeout(int seconds) finished");
        }
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::getLoginTimeout() started");
            return underlyingDataSource.getLoginTimeout();
        } finally {
            LOG.trace("PooledDataSourceImpl::getLoginTimeout() finished");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::unwrap(Class<?> iface) started");
            return underlyingDataSource.unwrap(iface);
        } finally {
            LOG.trace("PooledDataSourceImpl::unwrap(Class<?> iface) finished");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::isWrapperFor(Class<?> iface) started");
            return underlyingDataSource.isWrapperFor(iface);
        } finally {
            LOG.trace("PooledDataSourceImpl::isWrapperFor(Class<?> iface) finished");
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        try {
            LOG.trace("PooledDataSourceImpl::getParentLogger() started");
            return underlyingDataSource.getParentLogger();
        } finally {
            LOG.trace("PooledDataSourceImpl::getParentLogger() finished");
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            LOG.trace("PooledDataSourceImpl::close() started");
            connectionPool.close();
            underlyingDataSource.close();
        } finally {
            LOG.trace("PooledDataSourceImpl::close() finished");
        }
    }
}
