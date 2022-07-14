package org.neo4j.driver;

import java.util.Optional;
import java.util.Set;

public class DriverQueryConfigBuilder {
    private ClusterMemberAccess _access;
    private Set<Bookmark> _bookmarks;
    private String _database;

    public DriverQueryConfigBuilder withClusterMemberAccess(ClusterMemberAccess clusterMemberAccess) {
        this._access = clusterMemberAccess;
        return this;
    }

    public DriverQueryConfigBuilder withBookmarks(Set<Bookmark> bookmarks) {
        this._bookmarks = bookmarks;
        return this;
    }

    public DriverQueryConfigBuilder withDatabase(String database) {
        this._database = database;
        return this;
    }

    public DriverQueryConfig build() {
        return new DriverQueryConfig(_access,
                Optional.of(_bookmarks),
                Optional.of(_database));
    }
}
