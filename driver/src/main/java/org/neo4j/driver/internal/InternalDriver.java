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

import java.awt.print.Book;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

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
    public Boolean supportsAutomaticClusterMemberAccess() {
        return Futures.blockingGet(supportsAutomaticClusterMemberAccessAsync());
    }

    @Override
    public CompletionStage<Boolean> supportsAutomaticClusterMemberAccessAsync() {
        return sessionFactory.supportsAutomaticQueryRouting();
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

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, DriverQueryConfig config) {
        if (!config.validate()) {
            throw new IllegalStateException("Config specified was not valid.");
        }
        var internalSession = new InternalAsyncSession(newSession(config.sessionConfig(this.bookmarksHolder)));
        var queryFuture = internalSession.queryAsync(query, config.sessionQueryConfig());
        var bookmarkFuture = queryFuture.thenRun(() -> updateBookmarks(internalSession));
        return bookmarkFuture.thenCombine(queryFuture, (_x, result) -> result);
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(String query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(
            String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(new Query(query, parameters), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public CompletionStage<QueryResult> queryAsync(Query query, ClusterMemberAccess clusterMemberAccess) {
        return this.queryAsync(query, x -> x.withClusterMemberAccess(clusterMemberAccess));
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
        return this.query(new Query(query), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, ClusterMemberAccess clusterMemberAccess) {
        return this.query(new Query(query, parameters), x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(Query query, ClusterMemberAccess clusterMemberAccess) {
        return this.query(query, x -> x.withClusterMemberAccess(clusterMemberAccess));
    }

    @Override
    public QueryResult query(String query, DriverQueryConfig config) {
        return this.query(new Query(query), config);
    }

    @Override
    public QueryResult query(String query, Map<String, Object> parameters, DriverQueryConfig config) {
        return this.query(new Query(query, parameters), config);
    }

    @Override
    public QueryResult query(Query query, DriverQueryConfig config) {
        return Futures.blockingGet(this.queryAsync(query, config));
    }

    private CompletionStage<Void> updateBookmarks(AsyncSession asyncSession) {
        this.bookmarksHolder.setBookmarks(asyncSession.lastBookmarks());
        return asyncSession.closeAsync();
    }

    private QueryResult query(Query query, Function<DriverQueryConfigBuilder, DriverQueryConfigBuilder> configBuilder) {
        return this.query(
                query, configBuilder.apply(DriverQueryConfig.builder()).build());
    }

    private CompletionStage<QueryResult> queryAsync(
            Query query, Function<DriverQueryConfigBuilder, DriverQueryConfigBuilder> configBuilder) {
        return this.queryAsync(
                query, configBuilder.apply(DriverQueryConfig.builder()).build());
    }
}
