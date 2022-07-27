package org.neo4j.driver;

import org.neo4j.driver.summary.ResultSummary;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.stream.Stream;

public record QueryResult(Record[] records, ResultSummary summary, String[] keys) {
    public Record single() throws IllegalStateException {
        if (records.length == 1)
            return records[0];
        throw new IllegalStateException("Result's records did not contain only a single record");
    }

    public Value scalar() throws IllegalStateException, ClassCastException {
        if (records.length != 1) {
            throw new IllegalStateException("Result's records did not contain only a single record");
        }
        var record = records[0];
        if (record.size() != 1){
            throw new IllegalStateException("Result's records contained more than a single column.");
        }

        return record.get(0);
    }

    public Stream<Record> stream() {
        return Arrays.stream(records);
    }
}
