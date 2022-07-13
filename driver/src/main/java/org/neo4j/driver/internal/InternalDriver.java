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

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.async.InternalAsyncSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.reactive.InternalReactiveSession;
import org.neo4j.driver.internal.reactive.InternalRxSession;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.TypeSystem;

public class InternalDriver implements Driver {
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final MetricsProvider metricsProvider;
    private final BookmarksHolder bookmarksHolder;

    InternalDriver(
            SecurityPlan securityPlan,
            SessionFactory sessionFactory,
            MetricsProvider metricsProvider,
            Logging logging) {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog(getClass());
        this.bookmarksHolder = new DefaultBookmarksHolder();
    }

    @Override
    public Session session() {
        return new InternalSession(newSession(SessionConfig.defaultConfig()));
    }

    @Override
    public Session session(SessionConfig sessionConfig) {
        return new InternalSession(newSession(sessionConfig));
    }

    @Override
    public RxSession rxSession(SessionConfig sessionConfig) {
        return new InternalRxSession(newSession(sessionConfig));
    }

    @Override
    public ReactiveSession reactiveSession(SessionConfig sessionConfig) {
        return new InternalReactiveSession(newSession(sessionConfig));
    }

    @Override
    public AsyncSession asyncSession() {
        return new InternalAsyncSession(newSession(SessionConfig.defaultConfig()));
    }

    @Override
    public AsyncSession asyncSession(SessionConfig sessionConfig) {
        return new InternalAsyncSession(newSession(sessionConfig));
    }

    @Override
    public Metrics metrics() {
        return metricsProvider.metrics();
    }

    @Override
    public boolean isMetricsEnabled() {
        return metricsProvider != DevNullMetricsProvider.INSTANCE;
    }

    @Override
    public boolean isEncrypted() {
        assertOpen();
        return securityPlan.requiresEncryption();
    }

    @Override
    public void close() {
        Futures.blockingGet(closeAsync());
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing driver instance %s", hashCode());
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    @Override
    public final TypeSystem defaultTypeSystem() {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync() {
        return sessionFactory.verifyConnectivity();
    }

    @Override
    public boolean supportsMultiDb() {
        return Futures.blockingGet(supportsMultiDbAsync());
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDbAsync() {
        return sessionFactory.supportsMultiDb();
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query), new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(
            String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query, parameters), new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(query, new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, DriverQueryConfig config) {
        return this.queryAsync(new Query(query), config);
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(
            String query, Map<String, Object> parameters, DriverQueryConfig config) {
        return this.queryAsync(new Query(query, parameters), config);
    }

    @Override
    public QueryResult query(String query, ClusterMemberAccess clusterMemberAccess) {
        return this.query(new Query(query), new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return this.query(new Query(query, parameters), new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public QueryResult query(Query query, ClusterMemberAccess clusterMemberAccess) {
        return this.query(query, new DriverQueryConfig(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, DriverQueryConfig config) {
        return this.query(new Query(query), config);
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, DriverQueryConfig config) {
        return this.query(new Query(query, parameters), config);
    }

    private CompletionStage<Void> updateBookmarks(AsyncSession inst) {
        this.bookmarksHolder.setBookmarks(inst.lastBookmarks());
        return inst.closeAsync();
    }

    private CompletionStage<QueryResult> executeQueryInCtxAsync(Query query, AsyncTransactionContext ctx) {
        var cursorFuture = ctx.runAsync(query);
        return cursorFuture
                .thenApplyAsync(ResultCursor::listAsync)
                .thenCombineAsync(
                        cursorFuture.thenApplyAsync(ResultCursor::consumeAsync),
                        (listCompletionStage, summaryCompletionStage) -> new Object() {
                            final ResultSummary summary = Futures.getNow(summaryCompletionStage);
                            final List<Record> records = Futures.getNow(listCompletionStage);
                        })
                .thenCombine(
                        cursorFuture.thenApply(ResultCursor::keys),
                        (x, y) ->
                                new QueryResult(x.records.toArray(new Record[0]), x.summary, y.toArray(new String[0])));
    }

    public QueryResult executeQueryBlocking(TransactionContext ctx, Query query) {
        var cursor = ctx.run(query);
        var records = cursor.list();
        var summary = cursor.consume();
        return new QueryResult(
                records.toArray(new Record[0]), summary, cursor.keys().toArray(new String[0]));
    }

    @Override
    public QueryResult query(Query query, DriverQueryConfig config) {
        if (!validateConfig(config)) {
            throw new IllegalStateException("Config specified was not valid.");
        }

        var inst = session(readSessionConfig(config));
        try {
            var access = config.access();

            if (access == ClusterMemberAccess.Automatic) {
                // not implemented yet;
                // is it bolt or ServerSideRouting enabled
                verifyConnectivity();
                return null;
            }

            if (access == ClusterMemberAccess.Readers) {
                return inst.executeRead(ctx -> executeQueryBlocking(ctx, query));
            } else {
                return inst.executeWrite(ctx -> executeQueryBlocking(ctx, query));
            }
        } finally {
            this.bookmarksHolder.setBookmark(inst.lastBookmark());
            inst.close();
        }
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, DriverQueryConfig config) {
        if (!validateConfig(config)) {
            throw new IllegalStateException("Config specified was not valid.");
        }
        var inst = asyncSession(readSessionConfig(config));

        if (config.access() == ClusterMemberAccess.Automatic) {
            // not implemented yet;
            return Futures.asCompletionStage(null);
        } else if (config.access() == ClusterMemberAccess.Readers) {
            return inst.executeReadAsync(ctx -> executeQueryInCtxAsync(query, ctx), readTxConfig(config))
                    .thenCombine(updateBookmarks(inst), (x, y) -> x);
        } else {
            return inst.executeWriteAsync(ctx -> executeQueryInCtxAsync(query, ctx), readTxConfig(config))
                    .thenCombine(updateBookmarks(inst), (x, y) -> x);
        }
    }

    private TransactionConfig readTxConfig(DriverQueryConfig config) {
        return TransactionConfig.builder().build();
    }

    private SessionConfig readSessionConfig(DriverQueryConfig config) {
        var builder = SessionConfig.builder();
        builder.withBookmarks(bookmarksHolder.getBookmarks());
        return builder.build();
    }

    private boolean validateConfig(DriverQueryConfig config) {
        return true;
    }

    @Override
    public void verifyConnectivity() {
        Futures.blockingGet(verifyConnectivityAsync());
    }

    /**
     * Get the underlying session factory.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the session factory used by this driver.
     */
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    private static RuntimeException driverCloseException() {
        return new IllegalStateException("This driver instance has already been closed");
    }

    public NetworkSession newSession(SessionConfig config) {
        assertOpen();
        NetworkSession session = sessionFactory.newInstance(config);
        if (closed.get()) {
            // session does not immediately acquire connection, it is fine to just throw
            throw driverCloseException();
        }
        return session;
    }

    private void assertOpen() {
        if (closed.get()) {
            throw driverCloseException();
        }
    }
}
