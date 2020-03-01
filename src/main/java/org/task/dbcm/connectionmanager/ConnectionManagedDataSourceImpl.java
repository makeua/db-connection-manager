package org.task.dbcm.connectionmanager;

import org.slf4j.LoggerFactory;
import org.task.dbcm.connectionmanager.datasource.CloseableDataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

final class ConnectionManagedDataSourceImpl implements ConnectionManagedDataSource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConnectionManagedDataSourceImpl.class);

    private final CloseableDataSource master;
    private final CloseableDataSource slave;

    private final ExecutorService checkerExecutorService;

    ConnectionManagedDataSourceImpl(CloseableDataSource master, CloseableDataSource slave, ExecutorService checkerExecutorService) {
        this.master = Objects.requireNonNull(master, "Master DataSource cannot be null");
        this.slave = Objects.requireNonNull(slave, "Slave DataSource cannot be null");
        this.checkerExecutorService = Objects.requireNonNull(checkerExecutorService, "CheckerExecutorService cannot be null");
    }

    @Override
    public void close() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::close() started");
            master.close();
            slave.close();
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::close() finished");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection() started");
            return null;
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection() finished");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
