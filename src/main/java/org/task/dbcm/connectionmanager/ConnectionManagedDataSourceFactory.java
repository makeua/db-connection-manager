package org.task.dbcm.connectionmanager;

import org.task.dbcm.connectionmanager.datasource.CloseableDataSource;
import org.task.dbcm.connectionmanager.datasource.ConnectionPoolConfig;
import org.task.dbcm.connectionmanager.datasource.PooledDataSourceFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConnectionManagedDataSourceFactory {
    private final PooledDataSourceFactory pooledDataSourceFactory = new PooledDataSourceFactory();

    public ConnectionManagedDataSource createConnectionManagedDataSource(
            CloseableDataSource master,
            CloseableDataSource slave) {

        return new ConnectionManagedDataSourceImpl(master, slave, createCheckerExecutorService());
    }

    public ConnectionManagedDataSource createConnectionManagedDataSourceWithPooling(
            CloseableDataSource master,
            ConnectionPoolConfig masterConnectionPoolConfig,
            CloseableDataSource slave,
            ConnectionPoolConfig slaveConnectionPoolConfig) {

        return new ConnectionManagedDataSourceImpl(
                pooledDataSourceFactory.createPooledDataSource(
                        master,
                        masterConnectionPoolConfig.toBuilder()
                                .maxPoolSize(masterConnectionPoolConfig.getMaxPoolSize() + 1)
                                .build()),
                pooledDataSourceFactory.createPooledDataSource(
                        slave,
                        slaveConnectionPoolConfig.toBuilder()
                                .maxPoolSize(slaveConnectionPoolConfig.getMaxPoolSize() + 1)
                                .build()),
                createCheckerExecutorService());
    }

    private ExecutorService createCheckerExecutorService() {
        return Executors.newFixedThreadPool(2);
    }
}
