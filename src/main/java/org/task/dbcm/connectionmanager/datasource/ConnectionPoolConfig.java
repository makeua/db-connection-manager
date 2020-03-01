package org.task.dbcm.connectionmanager.datasource;

import lombok.*;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Builder(toBuilder = true)
public final class ConnectionPoolConfig {
    @NonNull
    private final Long connectionTTL;
    @NonNull
    private final Integer maxPoolSize;
}
