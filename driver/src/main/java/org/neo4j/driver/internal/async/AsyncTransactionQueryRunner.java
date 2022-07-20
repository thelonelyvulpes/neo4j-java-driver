package org.neo4j.driver.internal.async;

import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.QueryResult;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface AsyncTransactionQueryRunner {
    CompletionStage<QueryResult> queryAsync(String query);
    CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters);
    CompletionStage<QueryResult> queryAsync(Query query);
    CompletionStage<QueryResult> queryAsync(String query, QueryConfig config);
    CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters, QueryConfig config);
    CompletionStage<QueryResult> queryAsync(Query query, QueryConfig config);
}

