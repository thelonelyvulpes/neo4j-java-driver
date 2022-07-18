package org.neo4j.driver;


import java.time.Duration;
import java.util.Map;
import java.util.Set;

public record DriverQueryConfig(
        ClusterMemberAccess access,
        Set<Bookmark> bookmarks,
        String database,
        Integer maxRetries,
        Duration timeout,
        Map<String, Object> metadata,
        Boolean skipRecords
        ) {
    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }

    public QueryConfig queryConfig() {
        return new QueryConfig(skipRecords);
    }

    public TransactionConfig transactionConfig() {
        var builder = TransactionConfig.builder();
        builder.withMetadata(metadata);
        builder.withTimeout(timeout);
        return builder.build();
    }
}
