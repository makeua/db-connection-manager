package org.task.dbcm.connectionmanager.datasource;

/**
 * Simple configuration interface for connection pool.
 */
public interface ConnectionPoolConfig {
    long getConnectionTTL();
    int getMaxPoolSize();
}
