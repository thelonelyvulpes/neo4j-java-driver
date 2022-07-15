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

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.util.Futures.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.*;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.ResultSummary;

public class InternalAsyncSession extends AsyncAbstractQueryRunner implements AsyncSession {
    private final NetworkSession session;

    public InternalAsyncSession(NetworkSession session) {
        this.session = session;
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        return runAsync(query, TransactionConfig.empty());
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query, TransactionConfig config) {
        return runAsync(query, emptyMap(), config);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(
            String query, Map<String, Object> parameters, TransactionConfig config) {
        return runAsync(new Query(query, parameters), config);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query, TransactionConfig config) {
        return session.runAsync(query, config);
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        return session.closeAsync();
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query, parameters), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(query, x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, SessionQueryConfig config) {
        return this.queryAsync(new Query(query), config);
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, Map<String, Object> parameters, SessionQueryConfig config) {
        return this.queryAsync(new Query(query, parameters), config);
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, SessionQueryConfig config) {
        return this.executeQueryAsync(query, config.clusterMemberAccess(), extractTxConfig(config));
    }

    private TransactionConfig extractTxConfig(SessionQueryConfig config) {
        var txCfgBuilder = TransactionConfig.builder();

        if (config.metadata() != null) {
            txCfgBuilder.withMetadata(config.metadata());
        }

        if (config.timeout() != null) {
            txCfgBuilder.withTimeout(config.timeout());
        }

        return txCfgBuilder.build();
    }

    private CompletionStage<QueryResult> queryAsync(Query query, Function<SessionQueryConfigBuilder, SessionQueryConfigBuilder> configBuilderFunction) {
        return queryAsync(query, configBuilderFunction.apply(new SessionQueryConfigBuilder()).build());
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync() {
        return beginTransactionAsync(TransactionConfig.empty());
    }

    @Override
    public CompletionStage<AsyncTransaction> beginTransactionAsync(TransactionConfig config) {
        return session.beginTransactionAsync(config).thenApply(InternalAsyncTransaction::new);
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work) {
        return readTransactionAsync(work, TransactionConfig.empty());
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync(
            AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config) {
        return transactionAsync(AccessMode.READ, work, config);
    }

    @Override
    public <T> CompletionStage<T> executeReadAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config) {
        return readTransactionAsync(tx -> callback.execute(new DelegatingAsyncTransactionContext(tx)), config);
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work) {
        return writeTransactionAsync(work, TransactionConfig.empty());
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync(
            AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config) {
        return transactionAsync(AccessMode.WRITE, work, config);
    }

    @Override
    public <T> CompletionStage<T> executeWriteAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config) {
        return writeTransactionAsync(tx -> callback.execute(new DelegatingAsyncTransactionContext(tx)), config);
    }

    @Override
    public Bookmark lastBookmark() {
        return InternalBookmark.from(session.lastBookmarks());
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return new HashSet<>(session.lastBookmarks());
    }

    private <T> CompletionStage<T> transactionAsync(
            AccessMode mode, AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config) {
        return session.retryLogic().retryAsync(() -> {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<UnmanagedTransaction> txFuture = session.beginTransactionAsync(mode, config);

            txFuture.whenComplete((tx, completionError) -> {
                Throwable error = Futures.completionExceptionCause(completionError);
                if (error != null) {
                    resultFuture.completeExceptionally(error);
                } else {
                    executeWork(resultFuture, tx, work);
                }
            });

            return resultFuture;
        });
    }

    private <T> void executeWork(
            CompletableFuture<T> resultFuture, UnmanagedTransaction tx, AsyncTransactionWork<CompletionStage<T>> work) {
        CompletionStage<T> workFuture = safeExecuteWork(tx, work);
        workFuture.whenComplete((result, completionError) -> {
            Throwable error = Futures.completionExceptionCause(completionError);
            if (error != null) {
                closeTxAfterFailedTransactionWork(tx, resultFuture, error);
            } else {
                closeTxAfterSucceededTransactionWork(tx, resultFuture, result);
            }
        });
    }

    private <T> CompletionStage<T> safeExecuteWork(
            UnmanagedTransaction tx, AsyncTransactionWork<CompletionStage<T>> work) {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try {
            CompletionStage<T> result = work.execute(new InternalAsyncTransaction(tx));

            // protect from given transaction function returning null
            return result == null ? completedWithNull() : result;
        } catch (Throwable workError) {
            // work threw an exception, wrap it in a future and proceed
            return failedFuture(workError);
        }
    }

    private <T> void closeTxAfterFailedTransactionWork(
            UnmanagedTransaction tx, CompletableFuture<T> resultFuture, Throwable error) {
        tx.closeAsync().whenComplete((ignored, rollbackError) -> {
            if (rollbackError != null) {
                error.addSuppressed(rollbackError);
            }
            resultFuture.completeExceptionally(error);
        });
    }

    private <T> void closeTxAfterSucceededTransactionWork(
            UnmanagedTransaction tx, CompletableFuture<T> resultFuture, T result) {
        tx.closeAsync(true).whenComplete((ignored, completionError) -> {
            Throwable commitError = Futures.completionExceptionCause(completionError);
            if (commitError != null) {
                resultFuture.completeExceptionally(commitError);
            } else {
                resultFuture.complete(result);
            }
        });
    }

    public CompletionStage<QueryResult> executeQueryAsync(Query query,
                                                     ClusterMemberAccess clusterMemberAccess,
                                                     TransactionConfig txConfig) {
        return switch (clusterMemberAccess) {
            case Automatic -> ValidateCanRouteAndExecute(query, txConfig);
            case Readers -> executeReadAsync(x -> x.queryAsync(query), txConfig);
            case Writers -> executeWriteAsync(x -> x.queryAsync(query), txConfig);
        };
    }

    private CompletionStage<QueryResult> ValidateCanRouteAndExecute(Query query, TransactionConfig txConfig) {
        return this.session
                .canAutoRouteQuery()
                .thenCompose(canRoute -> {
                    if (canRoute) {
                        return executeReadAsync(x -> x.queryAsync(query), txConfig);
                    }
                    throw new IllegalStateException("Server does not support Automatic ClusterMemberAccess");
                });
    }

}
