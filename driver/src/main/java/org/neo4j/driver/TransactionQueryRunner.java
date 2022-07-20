package org.neo4j.driver;

import java.util.Map;

public interface TransactionQueryRunner {
    QueryResult query(String query);
    QueryResult query(String query, Map<String, Object> parameters);
    QueryResult query(Query query);
    QueryResult query(String query, QueryConfig config);
    QueryResult query(String query, Map<String, Object> parameters, QueryConfig config);
    QueryResult query(Query query, QueryConfig config);
}
