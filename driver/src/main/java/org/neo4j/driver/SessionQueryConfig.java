package org.neo4j.driver;

import java.util.Map;

public record SessionQueryConfig(
        ClusterMemberAccess clusterMemberAccess,
        java.time.Duration timeout,
        Map<String, Object> metadata,
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
        return new QueryConfig(this.skipRecords);
    }
}
