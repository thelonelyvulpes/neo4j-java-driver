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
package org.neo4j.driver.internal;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.util.Futures;

import java.util.Map;

import static org.neo4j.driver.internal.util.Futures.getNow;

public class InternalTransaction extends AbstractQueryRunner implements Transaction {
    private final UnmanagedTransaction tx;

    public InternalTransaction(UnmanagedTransaction tx) {
        this.tx = tx;
    }

    @Override
    public void commit() {
        Futures.blockingGet(
                tx.commitAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while committing the transaction"));
    }

    @Override
    public void rollback() {
        Futures.blockingGet(
                tx.rollbackAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while rolling back the transaction"));
    }

    @Override
    public void close() {
        Futures.blockingGet(
                tx.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while closing the transaction"));
    }

    @Override
    public Result run(Query query) {
        ResultCursor cursor = Futures.blockingGet(
                tx.runAsync(query),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while running query in transaction"));
        return new InternalResult(tx.connection(), cursor);
    }

    @Override
    public boolean isOpen() {
        return tx.isOpen();
    }

    private void terminateConnectionOnThreadInterrupt(String reason) {
        tx.connection().terminateAndRelease(reason);
    }

    @Override
    public QueryResult query(String query) {
        return this.query(new Query(query), QueryConfig.defaultValue);
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters) {
        return this.query(new Query(query, parameters), QueryConfig.defaultValue);
    }

    @Override
    public QueryResult query(Query query) {
        return this.query(query, QueryConfig.defaultValue);
    }

    @Override
    public QueryResult query(String query, QueryConfig config) {
        return this.query(new Query(query), config);

    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, QueryConfig config) {
        return this.query(new Query(query, parameters), config);
    }

    @Override
    public QueryResult query(Query query, QueryConfig config) {
        var cursorFuture = tx.runAsync(query);
        if (config.skipRecords()) {
            return Futures.blockingGet(cursorFuture
                    .thenCompose(ResultCursor::consumeAsync)
                    .thenApply(value -> new QueryResult(new Record[0], value, new String[0])));
        }

        var listFuture = cursorFuture.thenCompose(ResultCursor::listAsync);
        var consumeFuture = listFuture.thenCompose(_x -> cursorFuture.thenCompose(ResultCursor::consumeAsync));
        return Futures.blockingGet(consumeFuture.thenCombine(
                listFuture,
                (summaryCompletionStage, listCompletionStage) ->
                        new QueryResult(listCompletionStage.toArray(new Record[0]),
                                summaryCompletionStage,
                                getNow(cursorFuture).keys().toArray(new String[0]))));
    }
}
