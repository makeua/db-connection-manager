package org.task.dbcm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.task.dbcm.connectionmanager.ConnectionManagedDataSource;
import org.task.dbcm.connectionmanager.ConnectionManagedDataSourceFactory;
import org.task.dbcm.connectionmanager.datasource.CloseableDataSource;
import org.task.dbcm.connectionmanager.datasource.CloseableDataSourceFactory;
import org.task.dbcm.connectionmanager.datasource.ConnectionPoolConfig;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Main.class);

    private final CloseableDataSourceFactory closeableDataSourceFactory = new CloseableDataSourceFactory();

    private final ConnectionManagedDataSourceFactory connectionManagedDataSourceFactory = new ConnectionManagedDataSourceFactory();

    public static void main(String[] args) throws SQLException {
        org.slf4j.Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Logger log = ((Logger) rootLogger);
        log.setLevel(Level.DEBUG);
        new Main().run();
    }

    public void run() throws SQLException {
        CloseableDataSource master = createMaster();
        CloseableDataSource slave = createSlave();

        ConnectionManagedDataSource connectionManagedDataSource = connectionManagedDataSourceFactory
                .createConnectionManagedDataSourceWithPooling(
                        master,
                        ConnectionPoolConfig.builder()
                                .connectionTTL(60_000L)
                                .maxPoolSize(8)
                                .build(),
                        slave,
                        ConnectionPoolConfig.builder()
                                .connectionTTL(60_000L)
                                .maxPoolSize(4)
                                .build());

        NamedParameterJdbcOperations jdbcOperations = new NamedParameterJdbcTemplate(connectionManagedDataSource);

        final var start = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(10);

        final var sql = ""
                + " SELECT COUNT(*)               "
                + " FROM T.A AS A                 "
                + " WHERE A.NAME ILIKE (:pattern) ";

        for (int i = 0; i < 2_000; i++) {
            final var a = i;
            es.submit(() -> {
                try {
                    LOG.info("task-{}", a);
                    LOG.info("result-{}", jdbcOperations
                            .queryForObject(sql,
                                    Map.of("pattern", "%" + a),
                                    Integer.class));
                } catch (Exception e) {
                    LOG.error("Error:", e);
                }
            });
        }

        try {
            es.shutdown();
            while (!es.awaitTermination(10, TimeUnit.MILLISECONDS)) ;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final var end = System.currentTimeMillis();

        connectionManagedDataSource.close();

        LOG.info("Time: {}", (end - start) / 1000.0);
    }

    private CloseableDataSource createMaster() {
        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setServerName("localhost");
        pgSimpleDataSource.setPortNumber(5432);
        pgSimpleDataSource.setDatabaseName("tz");
        pgSimpleDataSource.setUser("maks");
        pgSimpleDataSource.setPassword("maks");
        return closeableDataSourceFactory.createCloseableDataSource(pgSimpleDataSource);
    }

    private CloseableDataSource createSlave() {
        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setServerName("localhost");
        pgSimpleDataSource.setPortNumber(5431);
        pgSimpleDataSource.setDatabaseName("tz");
        pgSimpleDataSource.setUser("maks");
        pgSimpleDataSource.setPassword("maks");
        return closeableDataSourceFactory.createCloseableDataSource(pgSimpleDataSource);
    }
}
