package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.summary.ResultSummary;

public class Errors {
    Driver driver;

    public Errors(Driver driver) {
        this.driver = driver;
    }

    public ResultSummary before() {
        //create config.
        var config = SessionConfig.builder().withDatabase("neo4j").build();
        // open session with config.
        try(var session = driver.session(config)) {
            // open a transaction.
            return session.executeRead(x -> {
                //get results.
                Result cursor = x.run("MATH (n:User) RETURN n.name as name");

                // throws exception
                return cursor.consume();
            });
        }
    }

    public ResultSummary after() {
        //throws exception
        var result = driver.query("MATH (n:User) RETURN n.name as name");

        return result.summary();
    }
}
