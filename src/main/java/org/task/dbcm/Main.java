package org.task.dbcm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.task.dbcm.connectionmanager.datasource.*;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws SQLException {
        org.slf4j.Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Logger log = ((Logger) rootLogger);
        log.setLevel(Level.ALL);

        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setServerName("localhost");
        pgSimpleDataSource.setPortNumber(5432);
        pgSimpleDataSource.setDatabaseName("tz");
        pgSimpleDataSource.setUser("maks");
        pgSimpleDataSource.setPassword("maks");


        CloseableDataSourceFactory closeableDataSourceFactory = new CloseableDataSourceFactory();
        PooledDataSourceFactory pooledDataSourceFactory = new PooledDataSourceFactory();

        CloseableDataSource closeableDataSource = closeableDataSourceFactory.createCloseableDataSource(pgSimpleDataSource);

        PooledDataSource pooledDataSource = pooledDataSourceFactory.createPooledDataSource(closeableDataSource, new ConnectionPoolConfigImpl(60_000, 4));

        NamedParameterJdbcOperations jdbcOperations = new NamedParameterJdbcTemplate(pooledDataSource);

        final var start = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            final var a = i;
            es.submit(() -> {
                LOG.info("task-{}-{}", a, jdbcOperations.queryForObject("select count(*) from t.a as A where A.name ilike ('%" + a + "')", Map.of(), Integer.class));
            });
        }

        try {
            es.shutdown();
            while (!es.awaitTermination(10, TimeUnit.MILLISECONDS)) ;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final var end = System.currentTimeMillis();

        pooledDataSource.close();

        LOG.info("Time: {}", (end - start) / 1000.0);
    }
}
