package org.neo4j.driver;

public record QueryConfig(
        int maxRecordCount, Boolean skipRecords) {
    public final static QueryConfig defaultValue = new QueryConfig(1000, false);

    public static QueryConfigBuilder builder() {
        return new QueryConfigBuilder();
    }
}
