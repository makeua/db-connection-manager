package org.task.dbcm.connectionmanager.datasource;

import java.util.Objects;

/**
 * A pooled connection key to store connection retrieved from the data source with/without credentials.
 */
class PooledConnectionKey {
    private final boolean credentials;
    private final String username;
    private final String password;

    public PooledConnectionKey() {
        this.credentials = false;
        this.username = null;
        this.password = null;
    }

    public PooledConnectionKey(String username, String password) {
        this.credentials = true;
        this.username = username;
        this.password = password;
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentials, username, password);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof PooledConnectionKey) && equals((PooledConnectionKey) obj);
    }

    private boolean equals(PooledConnectionKey that) {
        return Objects.equals(credentials, that.credentials)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
    }

    public boolean isCredentials() {
        return credentials;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
