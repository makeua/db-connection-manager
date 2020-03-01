package org.task.dbcm.connectionmanager.datasource;

import javax.sql.DataSource;

public class PooledDataSourceFactory {
    private final CloseableDataSourceFactory closeableDataSourceFactory = new CloseableDataSourceFactory();

    public PooledDataSource createPooledDataSource(DataSource dataSource, ConnectionPoolConfig connectionPoolConfig) {
        return createPooledDataSource(closeableDataSourceFactory.createCloseableDataSource(dataSource), connectionPoolConfig);
    }

    public PooledDataSource createPooledDataSource(CloseableDataSource closeableDataSource, ConnectionPoolConfig connectionPoolConfig) {
        ConnectionPool connectionPool = new ConnectionPoolImpl(
                closeableDataSource::getConnection,
                closeableDataSource::getConnection,
                connectionPoolConfig.getConnectionTTL(),
                connectionPoolConfig.getMaxPoolSize());
        return new PooledDataSourceImpl(closeableDataSource, connectionPool);
    }
}
