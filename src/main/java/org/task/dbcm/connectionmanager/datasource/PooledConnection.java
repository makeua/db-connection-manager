package org.task.dbcm.connectionmanager.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A wrapper around a connection to delegate all calls to wrapped connection
 * and to return a connection to the on close method call.
 */
final class PooledConnection implements Connection {
    private static final Logger LOG = LoggerFactory.getLogger(PooledConnection.class);

    private final PooledConnectionKey pooledConnectionKey;
    private final long creationTime;
    private final ConnectionPoolImpl connectionPool;

    private final Connection underlyingConnection;

    PooledConnection(
            PooledConnectionKey pooledConnectionKey,
            long creationTime,
            ConnectionPoolImpl connectionPool,

            Connection underlyingConnection) {
        this.pooledConnectionKey = pooledConnectionKey;
        this.creationTime = creationTime;
        this.connectionPool = connectionPool;

        this.underlyingConnection = underlyingConnection;
    }

    public PooledConnectionKey getPooledConnectionKey() {
        return pooledConnectionKey;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Connection getUnderlyingConnection() {
        return underlyingConnection;
    }

    @Override
    public void close() {
        try {
            LOG.trace("PooledConnection::close() started");
            connectionPool.returnConnection(this);
        } finally {
            LOG.trace("PooledConnection::close() finished");
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return underlyingConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return underlyingConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return underlyingConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return underlyingConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        underlyingConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return underlyingConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        underlyingConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        underlyingConnection.rollback();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return underlyingConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return underlyingConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        underlyingConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return underlyingConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        underlyingConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return underlyingConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        underlyingConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return underlyingConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return underlyingConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        underlyingConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return underlyingConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return underlyingConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return underlyingConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return underlyingConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        underlyingConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        underlyingConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return underlyingConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return underlyingConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return underlyingConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        underlyingConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        underlyingConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return underlyingConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return underlyingConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return underlyingConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return underlyingConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return underlyingConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return underlyingConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return underlyingConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return underlyingConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return underlyingConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return underlyingConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return underlyingConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        underlyingConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        underlyingConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return underlyingConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return underlyingConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return underlyingConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return underlyingConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        underlyingConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return underlyingConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        underlyingConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        underlyingConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return underlyingConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return underlyingConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return underlyingConnection.isWrapperFor(iface);
    }
}
