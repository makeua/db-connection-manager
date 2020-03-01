package org.task.dbcm.connectionmanager.datasource;

public class PooledDataSourceFactory {
    public PooledDataSource createPooledDataSource(CloseableDataSource closeableDataSource, ConnectionPoolConfig connectionPoolConfig) {
        ConnectionPool connectionPool = new ConnectionPoolImpl(closeableDataSource::getConnection,
                closeableDataSource::getConnection,
                connectionPoolConfig.getConnectionTTL(),
                connectionPoolConfig.getMaxPoolSize());
        return new PooledDataSourceImpl(closeableDataSource, connectionPool);
    }
}
