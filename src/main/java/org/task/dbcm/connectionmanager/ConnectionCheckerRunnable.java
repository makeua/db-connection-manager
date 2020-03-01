package org.task.dbcm.connectionmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.task.dbcm.connectionmanager.datasource.CloseableDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class ConnectionCheckerRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionCheckerRunnable.class);

    private final AtomicBoolean working = new AtomicBoolean(true);

    private final CloseableDataSource dataSource;
    private final AtomicBoolean statusHolder;

    public ConnectionCheckerRunnable(CloseableDataSource dataSource, AtomicBoolean statusHolder) {
        this.dataSource = Objects.requireNonNull(dataSource, "DataSource cannot be null");
        this.statusHolder = Objects.requireNonNull(statusHolder, "StatusHolder cannot be null");
    }

    @Override
    public void run() {
        while (working.get()) {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement("select 1");
                 ResultSet rs = ps.executeQuery()) {
                statusHolder.set(true);
            } catch (SQLException e) {
                statusHolder.set(false);
                LOG.debug("DataSource check has failed:", e);
            }
        }
    }

    public void stop() {
        working.set(false);
    }
}
