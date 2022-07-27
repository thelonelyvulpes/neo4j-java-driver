package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TxClusterMemberAccess;

import java.util.Map;

public class UnitOfWork {
    Driver driver;

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
                    // celebrateBirthday(name, id);

                    var updateCursor = x.run("MATCH (n:User {id: $id}) UPDATE n.celebrated = true");
                    updateCursor.consume();
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
                // celebrateBirthday(name, id);

                var updateCursor = x.query("MATCH (n:User {id: $id}) UPDATE n.celebrated = true");
            }

            return 1;
        }, TxClusterMemberAccess.Writers);
    }
}
