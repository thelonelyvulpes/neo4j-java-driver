package org.neo4j.driver;

public record QueryConfig(
        long maxRecordCount, Boolean skipRecords) {
}
