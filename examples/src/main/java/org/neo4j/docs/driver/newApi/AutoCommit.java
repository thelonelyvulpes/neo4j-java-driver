package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.util.List;
import java.util.Map;

public class AutoCommit {
    Driver driver;
    String[] values = { "archimedes", "david", "james" };

    public AutoCommit(Driver driver) {
        this.driver = driver;
    }

    public List<Record> before() {
        //create config.
        var config = SessionConfig.builder().withDatabase("neo4j").build();
        // open session with config.
        try (var session = driver.session(config)) {
            return session
                    .run("UNWIND $values AS v CALL { WITH v MERGE (u: Unit { value: v }) RETURN u } RETURN u, v",
                            Map.of("values", values))
                    .list();
        }
    }

    public Record[] after() {
        // use auto commit config.
        return driver
                .query("UNWIND $values AS v CALL { WITH v MERGE (u: Unit { value: v }) RETURN u } RETURN u, v",
                        Map.of("values", values),
                        DriverQueryConfig.autoCommit)
                .records();
    }

    public Record[] afterInSession() {
        var config = SessionConfig.builder().withDatabase("neo4j").build();

        // use auto commit config.
        try (var session = driver.session()) {
            return session.query(
                            "UNWIND $values AS v CALL { WITH v MERGE (u: Unit { value: v }) RETURN u } RETURN u, v",
                            Map.of("values", values),
                            SessionQueryConfig.autoCommit)
                    .records();
        }
    }
}
