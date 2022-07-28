package org.neo4j.driver;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DriverTxConfigBuilder {
    private String database;
    private Set<Bookmark> bookmarks;
    private String impersonatedUser;

    private TxClusterMemberAccess access;
    private Integer maxRetries;
    private Duration timeout;
    private Map<String, Object> metadata;
    private Function<RetryInfo, RetryDelay> retryFunction;

    public DriverTxConfigBuilder() {
        this(DriverTxConfig.read);
    }

    public DriverTxConfigBuilder(DriverTxConfig config) {
        this.database = config.database();
        this.bookmarks = config.bookmarks();
        this.impersonatedUser = config.impersonatedUser();
        this.access = config.sessionTxConfig().clusterMemberAccess();
        this.maxRetries = config.sessionTxConfig().maxRetries();
        this.timeout = config.sessionTxConfig().timeout();
        this.metadata = config.sessionTxConfig().metadata();
        this.retryFunction = config.sessionTxConfig().retryFunction();
    }

    public DriverTxConfigBuilder withClusterMemberAccess(TxClusterMemberAccess clusterMemberAccess) {
        this.access = clusterMemberAccess;
        return this;
    }

    public DriverTxConfigBuilder withBookmarks(Set<Bookmark> bookmarks) {
        this.bookmarks = bookmarks;
        return this;
    }

    public DriverTxConfigBuilder withDatabase(String database) {
        this.database = database;
        return this;
    }

    public DriverTxConfigBuilder withImpersonatedUser(String impersonatedUser) {
        this.impersonatedUser = impersonatedUser;
        return this;
    }

    public DriverTxConfigBuilder withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public DriverTxConfigBuilder withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public DriverTxConfigBuilder withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public DriverTxConfigBuilder withRetryFunction(Function<RetryInfo, RetryDelay> retryFunction) {
        this.retryFunction = retryFunction;
        return this;
    }

    public DriverTxConfig build() {
        return new DriverTxConfig(
                database,
                bookmarks,
                impersonatedUser,
                new SessionTxConfig(
                        access,
                        timeout,
                        metadata,
                        maxRetries,
                        retryFunction));
    }
}
