package org.neo4j.driver;

import java.util.Optional;
import java.util.Set;

public record DriverQueryConfig(
        ClusterMemberAccess access,
        Optional<Set<Bookmark>> bookmarks,
        Optional<String> database) {

    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }
}

