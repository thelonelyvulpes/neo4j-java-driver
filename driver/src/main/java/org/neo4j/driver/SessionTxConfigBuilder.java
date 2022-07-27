package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

public class SessionTxConfigBuilder {
    private TxClusterMemberAccess clusterMemberAccess;
    private Duration timeout;
    private Map<String, Object> metadata;
    private Integer maxRetries;
    private Function<RetryInfo, RetryDelay> retryFunction;

    public SessionTxConfigBuilder() {
        this(SessionTxConfig.read);
    }

    public SessionTxConfigBuilder(SessionTxConfig fromConfig) {
        this.clusterMemberAccess = fromConfig.clusterMemberAccess();
        this.timeout = fromConfig.timeout();
        this.metadata = fromConfig.metadata();
        this.maxRetries = fromConfig.maxRetries();
        this.retryFunction = fromConfig.retryFunction();
    }

    public SessionTxConfigBuilder withClusterMemberAccess(TxClusterMemberAccess clusterMemberAccess) {
        this.clusterMemberAccess = clusterMemberAccess;
        return this;
    }

    public SessionTxConfigBuilder withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public SessionTxConfigBuilder withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public SessionTxConfigBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public SessionTxConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction) {
        this.retryFunction = retryFunction;
        return this;
    }

    public SessionTxConfig build() {
        return new SessionTxConfig(
                clusterMemberAccess,
                timeout,
                metadata,
                maxRetries,
                retryFunction);
    }
}
