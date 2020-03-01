package org.task.dbcm.connectionmanager.datasource;

/**
 * A default implementation of the connection pool config.
 */
public final class ConnectionPoolConfigImpl implements ConnectionPoolConfig {
    private final long connectionTTL;
    private final int maxPoolSize;

    public ConnectionPoolConfigImpl(long connectionTTL, int maxPoolSize) {
        this.connectionTTL = connectionTTL;
        this.maxPoolSize = maxPoolSize;
    }

    @Override
    public long getConnectionTTL() {
        return connectionTTL;
    }

    @Override
    public int getMaxPoolSize() {
        return maxPoolSize;
    }
}
