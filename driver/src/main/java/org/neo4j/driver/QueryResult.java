package org.neo4j.driver;

import org.neo4j.driver.summary.ResultSummary;

public record QueryResult(Record[] records, ResultSummary summary, String[] keys) {
    public Record single() throws IllegalStateException {
        if (records.length == 1)
            return records[0];
        throw new IllegalStateException("Result's records did not contain only a single record");
    }

    public <TResult> TResult scalar() throws IllegalStateException, ClassCastException {
        if (records.length != 1) {
            throw new IllegalStateException("Result's records did not contain only a single record");
        }
        var record = records[0];
        if (record.size() != 1){
            throw new IllegalStateException("Result's records contained more than a single column.");
        }

        return (TResult) record.get(0);
    }
}
