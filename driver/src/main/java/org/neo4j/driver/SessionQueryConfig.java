package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

public record SessionQueryConfig(
        ClusterMemberAccess clusterMemberAccess,
        Boolean executeInTransaction,
        java.time.Duration timeout,
        Map<String, Object> metadata,
        Integer maxRetries,
        Function<RetryInfo, RetryDelay> retryFunction,
        QueryConfig queryConfig) {

    public static SessionQueryConfigBuilder builder() {
        return new SessionQueryConfigBuilder();
    }

    public static SessionQueryConfigBuilder builder(SessionQueryConfig from) {
        return new SessionQueryConfigBuilder(from);
    }

    public TransactionConfig transactionConfig() {
        var txCfgBuilder = TransactionConfig.builder();

        if (this.metadata() != null) {
            txCfgBuilder.withMetadata(this.metadata());
        }

        if (this.timeout() != null) {
            txCfgBuilder.withTimeout(this.timeout());
        }

        return txCfgBuilder.build();
    }

    public static final SessionQueryConfig defaultInstance = new SessionQueryConfig(
            ClusterMemberAccess.Automatic,
            true,
            Duration.ZERO,
            null,
            2,
            Retries.transientFunctions,
            QueryConfig.defaultValue);

    public static final SessionQueryConfig read = new SessionQueryConfig(
            ClusterMemberAccess.Readers,
            true,
            Duration.ZERO,
            null,
            2,
            Retries.transientFunctions,
            QueryConfig.defaultValue);

    public static final SessionQueryConfig write = new SessionQueryConfig(
            ClusterMemberAccess.Writers, true, Duration.ZERO, null, 2, Retries.noRetry, QueryConfig.defaultValue);

    public static final SessionQueryConfig autoCommit = new SessionQueryConfig(
            ClusterMemberAccess.Writers, false, Duration.ZERO, null, 0, Retries.noRetry, QueryConfig.defaultValue);

    public boolean validate() {
        return true;
    }
}
