package org.task.dbcm.connectionmanager.datasource;

import java.sql.SQLException;

@FunctionalInterface
public interface ClosingConsumer<D> {
    void accept(D dataSource) throws SQLException;
}
