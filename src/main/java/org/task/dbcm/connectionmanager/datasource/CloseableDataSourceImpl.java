package org.task.dbcm.connectionmanager.datasource;

import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * This is a wrapper implementation around a given data source which might have the possibility to be closed.
 * All the method calls are delegated to the underlying data source.
 * Close method invokes the closing consumer with the given data source.
 *
 * @param <D> the underlying data source type
 */
final class CloseableDataSourceImpl<D extends DataSource> implements CloseableDataSource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CloseableDataSourceImpl.class);

    private final D underlyingDataSource;
    private final ClosingConsumer<D> closingConsumer;

    CloseableDataSourceImpl(D underlyingDataSource, ClosingConsumer<D> closingConsumer) {
        this.underlyingDataSource = Objects.requireNonNull(underlyingDataSource, "Underlying DataSource cannot be null");
        this.closingConsumer = closingConsumer;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::getConnection() started");
            return underlyingDataSource.getConnection();
        } finally {
            LOG.trace("CloseableDataSourceImpl::getConnection() finished");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::getConnection(String username, String password) started");
            return underlyingDataSource.getConnection(username, password);
        } finally {
            LOG.trace("CloseableDataSourceImpl::getConnection(String username, String password) finished");
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::getLogWriter started");
            return underlyingDataSource.getLogWriter();
        } finally {
            LOG.trace("CloseableDataSourceImpl::getLogWriter finished");
        }
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::setLogWriter(PrintWriter out) started");
            underlyingDataSource.setLogWriter(out);
        } finally {
            LOG.trace("CloseableDataSourceImpl::setLogWriter(PrintWriter out) finished");
        }
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::setLoginTimeout(int seconds) started");
            underlyingDataSource.setLoginTimeout(seconds);
        } finally {
            LOG.trace("CloseableDataSourceImpl::setLoginTimeout(int seconds) finished");
        }
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::getLoginTimeout() started");
            return underlyingDataSource.getLoginTimeout();
        } finally {
            LOG.trace("CloseableDataSourceImpl::getLoginTimeout() finished");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::unwrap(Class<T> iface) started");
            return underlyingDataSource.unwrap(iface);
        } finally {
            LOG.trace("CloseableDataSourceImpl::unwrap(Class<T> iface) finished");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::isWrapperFor(Class<?> iface) started");
            return underlyingDataSource.isWrapperFor(iface);
        } finally {
            LOG.trace("CloseableDataSourceImpl::isWrapperFor(Class<?> iface) finished");
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        try {
            LOG.trace("CloseableDataSourceImpl::getParentLogger() started");
            return underlyingDataSource.getParentLogger();
        } finally {
            LOG.trace("CloseableDataSourceImpl::getParentLogger() finished");
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            LOG.trace("CloseableDataSourceImpl::close() started");
            if (closingConsumer != null) {
                closingConsumer.accept(underlyingDataSource);
            }
        } finally {
            LOG.trace("CloseableDataSourceImpl::close() finished");
        }
    }
}
