package org.neo4j.driver;


import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public record DriverQueryConfig(
        ClusterMemberAccess access,
        String database,
        Set<Bookmark> bookmarks,
        Integer maxRetries,
        Function<RetryInfo, RetryDelay> retryFunction,
        Duration timeout,
        Map<String, Object> metadata,
        Integer maxRecordCount,
        Boolean skipRecords
        ) {
    public static final Function<RetryInfo, RetryDelay> transientFunctions = info ->
            info.attempts() > info.maxRetry()
                    ? new RetryDelay(true, Duration.ofMillis(info.attempts() * 100))
                    : new RetryDelay(false, Duration.ZERO);

    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }

    public QueryConfig queryConfig() {
        return new QueryConfig(this.maxRecordCount, skipRecords);
    }

    public TransactionConfig transactionConfig() {
        var builder = TransactionConfig.builder();
        builder.withMetadata(metadata);
        builder.withTimeout(timeout);
        return builder.build();
    }
}
