package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public class DriverQueryConfigBuilder {
    private ClusterMemberAccess access = ClusterMemberAccess.Automatic;
    private Set<Bookmark> bookmarks = null;
    private String database = null;
    private Integer maxRetries = null;
    private Duration timeout = null;
    private Boolean skipRecords = false;
    private Map<String, Object> metadata = null;

    public DriverQueryConfigBuilder withClusterMemberAccess(ClusterMemberAccess clusterMemberAccess) {
        this.access = clusterMemberAccess;
        return this;
    }

    public DriverQueryConfigBuilder withBookmarks(Set<Bookmark> bookmarks) {
        this.bookmarks = bookmarks;
        return this;
    }

    public DriverQueryConfigBuilder withDatabase(String database) {
        this.database = database;
        return this;
    }

    public DriverQueryConfigBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public DriverQueryConfigBuilder withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public DriverQueryConfigBuilder withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public DriverQueryConfigBuilder withSkipRecords(boolean skipRecords){
        this.skipRecords = skipRecords;
        return this;
    }


    public DriverQueryConfig build() {
        return new DriverQueryConfig(access,
                bookmarks,
                database,
                maxRetries,
                timeout,
                metadata,
                skipRecords
                );
    }

}
