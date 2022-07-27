package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TxClusterMemberAccess;

import java.util.Map;

public class UnitOfWork {
    Driver driver;

    private void celebrateBirthday(String name, long id) {
        System.out.printf("it's it's user:%d, %s's brithday%n", id, name);
    }

    public UnitOfWork(Driver driver) {
        this.driver = driver;
    }

    public void before() {
        //create config.
        var config = SessionConfig.builder().withDatabase("neo4j").build();

        // open session with config.
        try (var session = driver.session(config)) {

            // open a transaction.
            session.executeWrite(x -> {

                //get results.
                var cursor = x.run(
                        "MATCH (n:User { dob: $dob, celebrated: false }) RETURN n.name as name, n.id as id",
                        Map.of("dob", java.time.MonthDay.now()));

                // do some activity
                for (var user : cursor.list()) {
                    var name = user.get("name").asString();
                    var id = user.get("id").asLong();
                    celebrateBirthday(name, id);

                    x.run("MATCH (n:User {id: $id}) SET n.celebrated = true").consume();
                }

                return 1;
            });
        }
    }

    public void after() {
        driver.execute(tx -> {
            //get results.
            var match = tx.query(
                    "MATCH (n:User { dob: $dob, celebrated: false }) RETURN n.name as name, n.id as id",
                    Map.of("dob", java.time.MonthDay.now()));

            // do some activity
            for (var user : match.records()) {
                var name = user.get("name").asString();
                var id = user.get("id").asLong();
                celebrateBirthday(name, id);

                // consume but don't read results.
                tx.query("MATCH (n:User {id: $id}) SET n.celebrated = true",
                        new QueryConfig(0, true));
            }

            return 1;
        }, TxClusterMemberAccess.Writers);
    }

    public void mixing() {
        //create config.
        var config = SessionConfig.builder().withDatabase("neo4j").build();

        // open session with config.
        try (var session = driver.session(config)) {

            // open a transaction.
            session.executeWrite(x -> {

                //get results.
                var results = x.query(
                        "MATCH (n:User { dob: $dob, celebrated: false }) RETURN n.name as name, n.id as id",
                        Map.of("dob", java.time.MonthDay.now()));

                // do some activity
                for (var user : results.records()) {
                    var name = user.get("name").asString();
                    var id = user.get("id").asLong();
                    celebrateBirthday(name, id);

                    x.query("MATCH (n:User {id: $id}) SET n.celebrated = true", new QueryConfig(0, true));
                }

                return 1;
            });
        }
    }

    public void illegalAccess() {
        try (var session = driver.session()) {
            var result = session.executeWrite(x -> x.run(
                    "MATCH (n:User { dob: $dob, celebrated: false }) RETURN n.name as name, n.id as id",
                    Map.of("dob", java.time.MonthDay.now())));

            // EXCEPTION!
            for (var user : result.list()) {
                System.out.printf("User: %s has id %d", user.get("name"), user.get("id").asLong());
            }
        }
    }

    public void noMoreIllegalAccess() {
        try (var session = driver.session()) {
            // open a transaction.
            var users = session.executeWrite(x -> x.query(
                    "MATCH (n:User { dob: $dob, celebrated: false }) RETURN n.name as name, n.id as id",
                    Map.of("dob", java.time.MonthDay.now())));

            // results are buffered into an array, no worries!
            for (var user : users.records()) {
                System.out.printf("User: %s has id %d", user.get("name"), user.get("id").asLong());
            }
        }
    }
}
