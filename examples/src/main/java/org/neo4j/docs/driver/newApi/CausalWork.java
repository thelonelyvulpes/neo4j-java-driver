package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;

import java.util.Map;
import java.util.Set;

public class CausalWork {
    private Driver driver;
    long userId = 10;

    public CausalWork(Driver driver) {
        this.driver = driver;
    }

    public void before() {
        Set<Bookmark> bookmarks;
        try (var session = driver.session()){
            session.executeWrite(x -> x.run("MERGE (u:User { id: $id })", Map.of("id", userId)));
            bookmarks = session.lastBookmarks();
        }

        var config = SessionConfig.builder()
                .withBookmarks(bookmarks).build();

        try (var session = driver.session(config)){
            session.executeRead(x -> x.run("MATCH (u:User { id: $id }) RETURN u.id", Map.of("id", userId))
                    .single().get(0).asLong());
        }
    }

    public Long after() {
        driver.query("MERGE (u:User { id: $id })", Map.of("id", userId));
        return driver.query("MATCH (u:User { id: $id }) RETURN u.id", Map.of("id", userId))
                .scalar().asLong();
    }
}
