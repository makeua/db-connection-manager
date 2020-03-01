package org.task.dbcm.connectionmanager.datasource;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPool extends AutoCloseable {
    Connection getConnection() throws SQLException;
    Connection getConnection(String username, String password) throws SQLException;

    @Override
    void close() throws SQLException;
}
