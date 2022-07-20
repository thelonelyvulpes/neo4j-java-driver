/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.async;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.QueryResult;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;

import static org.neo4j.driver.internal.util.Futures.getNow;

public class InternalAsyncTransaction extends AsyncAbstractQueryRunner implements AsyncTransaction {
    private final UnmanagedTransaction tx;

    public InternalAsyncTransaction(UnmanagedTransaction tx) {
        this.tx = tx;
    }

    @Override
    public CompletionStage<Void> commitAsync() {
        return tx.commitAsync();
    }

    @Override
    public CompletionStage<Void> rollbackAsync() {
        return tx.rollbackAsync();
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        return tx.closeAsync();
    }

    @Override
    public CompletionStage<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(isOpen());
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        return tx.runAsync(query);
    }

    public boolean isOpen() {
        return tx.isOpen();
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query) {
        return queryAsync(new Query(query), new QueryConfig(1000, false));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters) {
        return queryAsync(new Query(query, parameters), new QueryConfig(1000, false));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query) {
        return this.queryAsync(query, new QueryConfig(1000, false));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, QueryConfig config) {
        return this.queryAsync(new Query(query), config);

    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters, QueryConfig config) {
        return this.queryAsync(new Query(query, parameters), config);
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, QueryConfig config) {
        var cursorFuture = this.runAsync(query);
        if (config.skipRecords()) {
            return cursorFuture
                    .thenCompose(ResultCursor::consumeAsync)
                    .thenApply(value -> new QueryResult(new Record[0], value, new String[0]));
        }

        var listFuture = cursorFuture.thenCompose(ResultCursor::listAsync);
        var consumeFuture = listFuture.thenCompose(_x -> cursorFuture.thenCompose(ResultCursor::consumeAsync));
        return consumeFuture.thenCombine(
                listFuture,
                (summaryCompletionStage, listCompletionStage) ->
                        new QueryResult(listCompletionStage.toArray(new Record[0]),
                                summaryCompletionStage,
                                getNow(cursorFuture).keys().toArray(new String[0])));
    }
}

