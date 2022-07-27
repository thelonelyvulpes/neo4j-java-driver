package org.neo4j.driver;

import org.neo4j.driver.internal.BookmarksHolder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public record DriverTxConfig(String database, Set<Bookmark> bookmarks, SessionTxConfig sessionTxConfig) {

    public static DriverTxConfigBuilder builder() {
        return new DriverTxConfigBuilder();
    }

    public static DriverTxConfigBuilder builder(DriverTxConfig config) {
        return new DriverTxConfigBuilder(config);
    }

    public static final DriverTxConfig read = new DriverTxConfig("neo4j", null, SessionTxConfig.read);
    public static final DriverTxConfig write = new DriverTxConfig("neo4j", null, SessionTxConfig.write);

    public Optional<IllegalStateException> validate() {
        if (database == null || database.equals("")) {
            return Optional.of(new IllegalStateException("a database name must be defined"));
        }
        return sessionTxConfig.validate();
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

