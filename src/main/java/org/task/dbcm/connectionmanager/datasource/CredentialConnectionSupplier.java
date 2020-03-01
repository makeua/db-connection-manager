package org.task.dbcm.connectionmanager.datasource;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface CredentialConnectionSupplier {
    Connection get(String username, String password) throws SQLException;
}
