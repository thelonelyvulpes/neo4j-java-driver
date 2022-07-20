package org.neo4j.driver;

public class QueryConfigBuilder {
    private Boolean skipRecords = false;
    private Integer maxRecordCount = 1000;

    public QueryConfigBuilder withMaxRecordCount(int maxRecordCount){
        this.maxRecordCount = maxRecordCount;
        return this;
    }

    public QueryConfigBuilder withSkipRecords(boolean skipRecords){
        this.skipRecords = skipRecords;
        return this;
    }

    public QueryConfig build() {
        return new QueryConfig(maxRecordCount, skipRecords);
    }
}
