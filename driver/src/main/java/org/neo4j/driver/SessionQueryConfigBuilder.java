package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;

public class SessionQueryConfigBuilder {
    private Duration timeout;
    private ClusterMemberAccess clusterMemberAccess = ClusterMemberAccess.Automatic;
    private Map<String, Object> metadata;
    private Boolean skipRecords;

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

    public SessionQueryConfigBuilder withSkipRecords(boolean skipRecords){
        this.skipRecords = skipRecords;
        return this;
    }

    public SessionQueryConfig build() {
        return new SessionQueryConfig(clusterMemberAccess, timeout, metadata, skipRecords);
    }
}
