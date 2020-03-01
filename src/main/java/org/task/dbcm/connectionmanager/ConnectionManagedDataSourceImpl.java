package org.task.dbcm.connectionmanager;

import org.slf4j.LoggerFactory;
import org.task.dbcm.connectionmanager.datasource.CloseableDataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * A data source implementation which can use slave data source to get connections if the master is dead.
 * The check is performed in the execution service using a checking runnable which updates the status of the
 * master data source. If the master data source is up then all new connections are got from the
 * master data source.
 * A status checker runnable always uses one connection from the master data source to perform a
 * simple query to check the availability of the master data source. It does it all the time.
 * Executor service is used for status checker runnable cause it's easier.
 */
final class ConnectionManagedDataSourceImpl implements ConnectionManagedDataSource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConnectionManagedDataSourceImpl.class);

    private final CloseableDataSource master;
    private final CloseableDataSource slave;

    private final AtomicBoolean masterAlive;
    private final ExecutorService checkerExecutorService;
    private final ConnectionCheckerRunnable connectionCheckerRunnable;

    ConnectionManagedDataSourceImpl(CloseableDataSource master, CloseableDataSource slave, ExecutorService checkerExecutorService) {
        this.master = Objects.requireNonNull(master, "Master DataSource cannot be null");
        this.slave = Objects.requireNonNull(slave, "Slave DataSource cannot be null");

        this.masterAlive = new AtomicBoolean(true);
        this.checkerExecutorService = Objects.requireNonNull(checkerExecutorService, "CheckerExecutorService cannot be null");
        this.connectionCheckerRunnable = new ConnectionCheckerRunnable(master, masterAlive);

        this.checkerExecutorService.submit(connectionCheckerRunnable);
    }

    @Override
    public void close() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::close() started");
            connectionCheckerRunnable.stop();
            master.close();
            slave.close();

            checkerExecutorService.shutdown();
            if (!checkerExecutorService.awaitTermination(10_000, TimeUnit.MILLISECONDS)) {
                LOG.debug("Failed to stop the checker execution service");
                checkerExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.debug("Interrupted while shutting down the checker execution service:", e);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::close() finished");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection() started");
            return masterAlive.get() ? master.getConnection() : slave.getConnection();
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection() finished");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection(String username, String password) started");
            return masterAlive.get() ? master.getConnection(username, password) : slave.getConnection(username, password);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getConnection(String username, String password) finished");
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getLogWriter() started");
            return master.getLogWriter();
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getLogWriter() finished");
        }
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::setLogWriter(PrintWriter out) started");
            master.setLogWriter(out);
            slave.setLogWriter(out);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::setLogWriter(PrintWriter out) finished");
        }
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::setLoginTimeout(int seconds) started");
            master.setLoginTimeout(seconds);
            slave.setLoginTimeout(seconds);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::setLoginTimeout(int seconds) finished");
        }
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getLoginTimeout() started");
            return master.getLoginTimeout();
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getLoginTimeout() finished");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::unwrap(Class<T> iface) started");
            return master.unwrap(iface);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::unwrap(Class<T> iface) finished");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::isWrapperFor(Class<T> iface) started");
            return master.isWrapperFor(iface);
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::isWrapperFor(Class<T> iface) finished");
        }
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        try {
            LOG.trace("ConnectionManagedDataSourceImpl::getParentLogger() started");
            return master.getParentLogger();
        } finally {
            LOG.trace("ConnectionManagedDataSourceImpl::getParentLogger() finished");
        }
    }
}
