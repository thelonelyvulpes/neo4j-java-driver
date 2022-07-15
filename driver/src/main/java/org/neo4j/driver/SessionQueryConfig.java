package org.neo4j.driver;

import java.util.Map;

public record SessionQueryConfig(
        ClusterMemberAccess clusterMemberAccess,
        java.time.Duration timeout,
        Map<String, Object> metadata) {

    static SessionQueryConfigBuilder builder() {
        return new SessionQueryConfigBuilder();
    }
}
