package org.neo4j.driver;

import org.neo4j.driver.internal.BookmarksHolder;

import java.util.Set;

public record DriverQueryConfig(String database, Set<Bookmark> bookmarks, SessionQueryConfig sessionQueryConfig) {

    public static DriverQueryConfigBuilder builder() {
        return new DriverQueryConfigBuilder();
    }

    public static final DriverQueryConfig defaultInstance =
            new DriverQueryConfig("neo4j", null, SessionQueryConfig.defaultInstance);

    public static final DriverQueryConfig read = new DriverQueryConfig("neo4j", null, SessionQueryConfig.read);

    public static final DriverQueryConfig write = new DriverQueryConfig("neo4j", null, SessionQueryConfig.write);

    public static final DriverQueryConfig autoCommit =
            new DriverQueryConfig("neo4j", null, SessionQueryConfig.autoCommit);

    public boolean validate() {
        return sessionQueryConfig.validate();
    }

    public SessionConfig sessionConfig(BookmarksHolder bookmarksHolder) {
        SessionConfig.Builder builder = SessionConfig.builder();

        if (this.bookmarks() != null)
            builder.withBookmarks(this.bookmarks());
        else if (bookmarksHolder != null)
            builder.withBookmarks(bookmarksHolder.getBookmarks());

        if (this.database() != null)
            builder.withDatabase(this.database());

        return builder.build();
    }
}
