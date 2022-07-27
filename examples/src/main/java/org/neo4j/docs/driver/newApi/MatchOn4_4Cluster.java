package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.*;

import java.util.Arrays;

public class MatchOn4_4Cluster {
    Driver driver;

    public MatchOn4_4Cluster(Driver driver) {
        this.driver = driver;
    }

    public String[] before() {
        //create config.
        var config = SessionConfig.builder().withDatabase("neo4j").build();
        // open session with config.
        try (var session = driver.session(config)) {
            // open a transaction.
            return session.executeRead(x -> {
                //get results.
                Result cursor = x.run("MATCH (n:User) RETURN n.name as name");
                //map results.
                return (String[]) cursor.stream().map(y -> y.get(0).asString()).toArray();
            });
        }
    }

    public String[] after() {
        //get results.
        var result = driver.query("MATCH (n:User) RETURN n.name as name", ClusterMemberAccess.Readers);
        // map results.
        return (String[]) Arrays.stream(result.records()).map(x -> x.get(0).asString()).toArray();
    }
}

