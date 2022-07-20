package org.neo4j.driver;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DriverQueryConfigBuilder {
    private ClusterMemberAccess access = ClusterMemberAccess.Automatic;
    private Set<Bookmark> bookmarks = null;
    private String database = "neo4j";
    private Integer maxRetries = 2;
    private Duration timeout = Duration.ZERO;
    private Boolean skipRecords = false;
    private Map<String, Object> metadata = null;
    private int maxRecordCount = 1000;
    private Function<RetryInfo, RetryDelay> retryFunction = DriverQueryConfig.transientFunctions;

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

    public DriverQueryConfigBuilder withMaxRecordCount(int maxRecordCount) {
        this.maxRecordCount = maxRecordCount;
        return this;
    }

    public DriverQueryConfigBuilder withSkipRecords(boolean skipRecords){
        this.skipRecords = skipRecords;
        return this;
    }

    public DriverQueryConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction){
        this.retryFunction = retryFunction;
        return this;
    }


    public DriverQueryConfig build() {
        return new DriverQueryConfig(access,
                database,
                bookmarks,
                maxRetries,
                retryFunction,
                timeout,
                metadata,
                maxRecordCount,
                skipRecords
                );
    }

}
