package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;
import org.neo4j.driver.DriverQueryConfig;
import org.neo4j.driver.Result;
import org.neo4j.driver.SessionConfig;

import java.util.Arrays;

public class MatchMultiDb {
    Driver driver;

    public MatchMultiDb(Driver driver) {
        this.driver = driver;
    }

    public String[] before() {
        //create config.
        var config = SessionConfig.builder().withDatabase("Users").build();

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
        // create config object.
        // set read, and user db.
        var config = DriverQueryConfig.builder()
                .withDatabase("Users")
                .build();

        //get results.
        var result = driver.query("MATCH (n:User) RETURN n.name as name", config);

        // map results.
        return (String[]) result.stream().map(x -> x.get(0).asString()).toArray();
    }
}
