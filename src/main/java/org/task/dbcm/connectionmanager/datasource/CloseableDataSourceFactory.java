package org.task.dbcm.connectionmanager.datasource;

import javax.sql.DataSource;

public class CloseableDataSourceFactory {
    public <D extends DataSource> CloseableDataSource createCloseableDataSource(D dataSource) {
        return createCloseableDataSource(dataSource, null);
    }

    public <D extends DataSource> CloseableDataSource createCloseableDataSource(D dataSource, ClosingConsumer<D> closingConsumer) {
        return new CloseableDataSourceImpl<>(dataSource, closingConsumer);
    }
}
