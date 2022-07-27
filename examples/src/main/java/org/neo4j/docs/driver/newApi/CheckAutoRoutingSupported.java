package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;

public class CheckAutoRoutingSupported {
    private Driver driver;

    public CheckAutoRoutingSupported(Driver driver){
        this.driver = driver;
    }

    public boolean checkSupportForAutoRouting() {
        return driver.supportsAutomaticClusterMemberAccess();
    }
}
