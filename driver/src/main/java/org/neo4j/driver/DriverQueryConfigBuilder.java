package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DriverQueryConfigBuilder {
    private String impersonatedUser;
    private String database;
    private Set<Bookmark> bookmarks;

    private ClusterMemberAccess access;
    private Boolean executeInTransaction;
    private Integer maxRetries;
    private Duration timeout;
    private Map<String, Object> metadata;
    private Function<RetryInfo, RetryDelay> retryFunction;

    private int maxRecordCount;
    private Boolean skipRecords;

    public DriverQueryConfigBuilder() {
        this(DriverQueryConfig.defaultInstance);
    }

    public DriverQueryConfigBuilder(DriverQueryConfig config) {
        this.database = config.database();
        this.bookmarks = config.bookmarks();
        this.impersonatedUser = config.impersonatedUser();
        this.access = config.sessionQueryConfig().clusterMemberAccess();
        this.executeInTransaction = config.sessionQueryConfig().executeInTransaction();
        this.maxRetries = config.sessionQueryConfig().maxRetries();
        this.timeout = config.sessionQueryConfig().timeout();
        this.metadata = config.sessionQueryConfig().metadata();
        this.retryFunction = config.sessionQueryConfig().retryFunction();
        this.maxRecordCount = config.sessionQueryConfig().queryConfig().maxRecordCount();
        this.skipRecords = config.sessionQueryConfig().queryConfig().skipRecords();
    }

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

    public DriverQueryConfigBuilder withImpersonatedUser(String impersonatedUser) {
        this.impersonatedUser = impersonatedUser;
        return this;
    }

    public DriverQueryConfigBuilder withExecuteInTransaction(boolean executeInTransaction) {
        this.executeInTransaction = executeInTransaction;
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

    public DriverQueryConfigBuilder withSkipRecords(boolean skipRecords) {
        this.skipRecords = skipRecords;
        return this;
    }

    public DriverQueryConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction) {
        this.retryFunction = retryFunction;
        return this;
    }

    public DriverQueryConfig build() {
        return new DriverQueryConfig(
                database,
                bookmarks,
                impersonatedUser,
                new SessionQueryConfig(
                        access,
                        executeInTransaction,
                        timeout,
                        metadata,
                        maxRetries,
                        retryFunction,
                        new QueryConfig(maxRecordCount, skipRecords)));
    }
}
