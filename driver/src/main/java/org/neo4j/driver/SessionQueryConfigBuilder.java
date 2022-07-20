package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

public class SessionQueryConfigBuilder {
    private ClusterMemberAccess clusterMemberAccess = ClusterMemberAccess.Automatic;
    private Duration timeout = Duration.ZERO;
    private Map<String, Object> metadata = null;
    private Boolean skipRecords = false;
    private Integer maxRecordCount = 1000;
    private Function<RetryInfo, RetryDelay> retryFunction = DriverQueryConfig.transientFunctions;

    public SessionQueryConfigBuilder withClusterMemberAccess(ClusterMemberAccess clusterMemberAccess) {
        this.clusterMemberAccess = clusterMemberAccess;
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

    public SessionQueryConfigBuilder withMaxRecordCount(int maxRecordCount){
        this.maxRecordCount = maxRecordCount;
        return this;
    }

    public SessionQueryConfigBuilder withSkipRecords(boolean skipRecords){
        this.skipRecords = skipRecords;
        return this;
    }

    public SessionQueryConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction) {
        this.retryFunction = retryFunction;
        return this;
    }

    public SessionQueryConfig build() {
        return new SessionQueryConfig(clusterMemberAccess,
                timeout,
                metadata,
                retryFunction,
                maxRecordCount,
                skipRecords);
    }
}
