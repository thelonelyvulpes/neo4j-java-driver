package org.neo4j.driver;


import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public record DriverQueryConfig(
        ClusterMemberAccess access,
        Set<Bookmark> bookmarks,
        String database,
        Integer maxRetries,
        Duration timeout
        ) {
    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }
}

