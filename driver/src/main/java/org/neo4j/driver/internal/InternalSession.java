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

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.*;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;

public class InternalSession extends AbstractQueryRunner implements Session {
    private final NetworkSession session;

    public InternalSession(NetworkSession session) {
        this.session = session;
    }

    @Override
    public Result run(Query query) {
        return run(query, TransactionConfig.empty());
    }

    @Override
    public Result run(String query, TransactionConfig config) {
        return run(query, emptyMap(), config);
    }

    @Override
    public Result run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return run(new Query(query, parameters), config);
    }

    @Override
    public Result run(Query query, TransactionConfig config) {
        ResultCursor cursor = Futures.blockingGet(
                session.runAsync(query, config),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while running query in session"));

        // query executed, it is safe to obtain a connection in a blocking way
        Connection connection = Futures.getNow(session.connectionAsync());
        return new InternalResult(connection, cursor);
    }

    @Override
    public boolean isOpen() {
        return session.isOpen();
    }

    @Override
    public void close() {
        Futures.blockingGet(
                session.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while closing the session"));
    }

    private QueryResult query(Query query, Function<SessionQueryConfigBuilder, SessionQueryConfigBuilder> configBuilderFunction) {
        return query(query, configBuilderFunction.apply(new SessionQueryConfigBuilder()).build());
    }
    @Override
    public QueryResult query(String query, ClusterMemberAccess clusterMemberAccess) {
        return query(new Query(query), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return query(new Query(query, parameters), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(Query query, ClusterMemberAccess clusterMemberAccess) {
        return query(query, x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, SessionQueryConfig config) {
        return query(new Query(query), config);
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, SessionQueryConfig config) {
        return query(new Query(query, parameters), config);
    }

    @Override
    public QueryResult query(Query query, SessionQueryConfig config) {
        return executeQuery(query, config.clusterMemberAccess(),
                config.transactionConfig(), config.queryConfig());
    }

    public QueryResult executeQuery(Query query, ClusterMemberAccess clusterMemberAccess,
                                    TransactionConfig txConfig, QueryConfig queryConfig) {
        return switch (clusterMemberAccess) {
            case Automatic -> ValidateCanRouteAndExecute(query, txConfig, queryConfig);
            case Readers -> executeRead(x -> x.query(query, queryConfig), txConfig);
            case Writers -> executeWrite(x -> x.query(query, queryConfig), txConfig);
        };
    }
    private QueryResult ValidateCanRouteAndExecute(Query query, TransactionConfig txConfig, QueryConfig queryConfig) {
        return Futures.blockingGet(this.session
                .canAutoRouteQuery()
                .thenApply(canRoute -> {
                    if (canRoute) {
                        return executeRead(x -> x.query(query, queryConfig), txConfig);
                    }
                    throw new IllegalStateException("Server does not support Automatic ClusterMemberAccess");
                }));
    }


    @Override
    public Transaction beginTransaction() {
        return beginTransaction(TransactionConfig.empty());
    }

    @Override
    public Transaction beginTransaction(TransactionConfig config) {
        UnmanagedTransaction tx = Futures.blockingGet(
                session.beginTransactionAsync(config),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while starting a transaction"));
        return new InternalTransaction(tx);
    }

    @Override
    public <T> T readTransaction(TransactionWork<T> work) {
        return readTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> T readTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.READ, work, config);
    }

    @Override
    public <T> T executeRead(TransactionCallback<T> callback, TransactionConfig config) {
        return readTransaction(tx -> callback.execute(new DelegatingTransactionContext(tx)), config);
    }

    @Override
    public <T> T writeTransaction(TransactionWork<T> work) {
        return writeTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> T writeTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.WRITE, work, config);
    }

    @Override
    public <T> T executeWrite(TransactionCallback<T> callback, TransactionConfig config) {
        return writeTransaction(tx -> callback.execute(new DelegatingTransactionContext(tx)), config);
    }

    @Override
    public Bookmark lastBookmark() {
        return InternalBookmark.from(session.lastBookmarks());
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return session.lastBookmarks();
    }

    private <T> T transaction(AccessMode mode, TransactionWork<T> work, TransactionConfig config) {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        return session.retryLogic().retry(() -> {
            try (Transaction tx = beginTransaction(mode, config)) {

                T result = work.execute(tx);
                if (tx.isOpen()) {
                    // commit tx if a user has not explicitly committed or rolled back the transaction
                    tx.commit();
                }
                return result;
            }
        });
    }

    private Transaction beginTransaction(AccessMode mode, TransactionConfig config) {
        UnmanagedTransaction tx = Futures.blockingGet(
                session.beginTransactionAsync(mode, config),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while starting a transaction"));
        return new InternalTransaction(tx);
    }

    private void terminateConnectionOnThreadInterrupt(String reason) {
        // try to get current connection if it has been acquired
        Connection connection = null;
        try {
            connection = Futures.getNow(session.connectionAsync());
        } catch (Throwable ignore) {
            // ignore errors because handing interruptions is best effort
        }

        if (connection != null) {
            connection.terminateAndRelease(reason);
        }
    }
}
