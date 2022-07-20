package org.neo4j.driver;

import java.util.Map;
import java.util.function.Function;

public record SessionQueryConfig(
        ClusterMemberAccess clusterMemberAccess,
        java.time.Duration timeout,
        Map<String, Object> metadata,
        Function<RetryInfo, RetryDelay> retryFunction,
        Integer maxRecordCount,
        Boolean skipRecords) {

    public static SessionQueryConfigBuilder builder() {
        return new SessionQueryConfigBuilder();
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

    public QueryConfig queryConfig() {
        return new QueryConfig(this.maxRecordCount, this.skipRecords);
    }
}

