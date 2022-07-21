package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
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
            Duration.ofDays(1),
            Map.of(),
            2,
            Retries.transientFunctions,
            QueryConfig.defaultValue);

    public static final SessionQueryConfig read = new SessionQueryConfig(
            ClusterMemberAccess.Readers,
            true,
            Duration.ofDays(1),
            Map.of(),
            2,
            Retries.transientFunctions,
            QueryConfig.defaultValue);

    public static final SessionQueryConfig write = new SessionQueryConfig(
            ClusterMemberAccess.Writers, true, Duration.ofDays(1), Map.of(), 2, Retries.transientFunctions, QueryConfig.defaultValue);

    public static final SessionQueryConfig autoCommit = new SessionQueryConfig(
            ClusterMemberAccess.Writers, false, Duration.ofDays(1), null, 0, Retries.noRetry, QueryConfig.defaultValue);

    public Optional<IllegalStateException> validate() {
        if (!executeInTransaction && metadata != null) {
            return Optional.of(new IllegalStateException("can not define metadata when not executing in transaction."));
        }

        if (metadata == null) {
            return Optional.of(new IllegalStateException("Metadata for transaction should not be null."));
        }

        if (maxRetries < 0) {
            return Optional.of(new IllegalStateException("Can not define negative maximum retries."));
        }

        return QueryConfig.validate();
    }
}
