package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

public class SessionQueryConfigBuilder {
    private ClusterMemberAccess clusterMemberAccess;
    private Boolean executeInTransaction;
    private Duration timeout;
    private Map<String, Object> metadata;
    private Integer maxRetries;
    private Function<RetryInfo, RetryDelay> retryFunction;
    private Boolean skipRecords;
    private Integer maxRecordCount;

    public SessionQueryConfigBuilder() {
        this(SessionQueryConfig.defaultInstance);
    }

    public SessionQueryConfigBuilder(SessionQueryConfig fromConfig) {
        this.clusterMemberAccess = fromConfig.clusterMemberAccess();
        this.timeout = fromConfig.timeout();
        this.metadata = fromConfig.metadata();
        this.skipRecords = fromConfig.queryConfig().skipRecords();
        this.maxRecordCount = fromConfig.queryConfig().maxRecordCount();
        this.maxRetries = fromConfig.maxRetries();
        this.retryFunction = fromConfig.retryFunction();
    }

    public SessionQueryConfigBuilder withClusterMemberAccess(ClusterMemberAccess clusterMemberAccess) {
        this.clusterMemberAccess = clusterMemberAccess;
        return this;
    }

    public SessionQueryConfigBuilder withExecuteInTransaction(boolean executeInTransaction) {
        this.executeInTransaction = executeInTransaction;
        return this;
    }

    public SessionQueryConfigBuilder withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public SessionQueryConfigBuilder withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public SessionQueryConfigBuilder withMaxRecordCount(int maxRecordCount) {
        this.maxRecordCount = maxRecordCount;
        return this;
    }

    public SessionQueryConfigBuilder withSkipRecords(boolean skipRecords) {
        this.skipRecords = skipRecords;
        return this;
    }

    public SessionQueryConfigBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public SessionQueryConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction) {
        this.retryFunction = retryFunction;
        return this;
    }

    public SessionQueryConfig build() {
        return new SessionQueryConfig(
                clusterMemberAccess,
                executeInTransaction,
                timeout,
                metadata,
                maxRetries,
                retryFunction,
                new QueryConfig(maxRecordCount, skipRecords));
    }
}
