package org.task.dbcm.connectionmanager.datasource;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * An interface to wrap a data source along with it's close method (if possible).
 * Method close delegates the call to the close method of the wrapped data source.
 */
public interface CloseableDataSource extends DataSource, AutoCloseable {
    @Override
    void close() throws SQLException;
}
