/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.telemetry.TelemetryApi;
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
        var cursor = Futures.blockingGet(
                session.runAsync(query, config),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while running query in session"));

        // query executed, it is safe to obtain a connection in a blocking way
        var connection = Futures.getNow(session.connectionAsync());
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

    @Override
    public Transaction beginTransaction() {
        return beginTransaction(TransactionConfig.empty());
    }

    @Override
    public Transaction beginTransaction(TransactionConfig config) {
        return beginTransaction(config, null);
    }

    public Transaction beginTransaction(TransactionConfig config, String txType) {
        var span = GlobalOpenTelemetry.getTracer("driver", "5.15.0").spanBuilder("Transaction").startSpan();
        try (var scope = span.makeCurrent()) {
            var tx = Futures.blockingGet(
                    session.beginTransactionAsync(config, txType, new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION), span),
                    () -> terminateConnectionOnThreadInterrupt("Thread interrupted while starting a transaction"));
            return new InternalTransaction(tx, span);
        }
    }

    @Override
    @Deprecated
    public <T> T readTransaction(TransactionWork<T> work) {
        return readTransaction(work, TransactionConfig.empty());
    }

    @Override
    @Deprecated
    public <T> T readTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.READ, work, config, TelemetryApi.MANAGED_TRANSACTION, true);
    }

    @Override
    public <T> T executeRead(TransactionCallback<T> callback, TransactionConfig config) {
        return execute(AccessMode.READ, callback, config, TelemetryApi.MANAGED_TRANSACTION, true);
    }

    @Override
    @Deprecated
    public <T> T writeTransaction(TransactionWork<T> work) {
        return writeTransaction(work, TransactionConfig.empty());
    }

    @Override
    @Deprecated
    public <T> T writeTransaction(TransactionWork<T> work, TransactionConfig config) {
        return transaction(AccessMode.WRITE, work, config, TelemetryApi.MANAGED_TRANSACTION, true);
    }

    @Override
    public <T> T executeWrite(TransactionCallback<T> callback, TransactionConfig config) {
        return execute(AccessMode.WRITE, callback, config, TelemetryApi.MANAGED_TRANSACTION, true);
    }

    @Override
    @Deprecated
    public Bookmark lastBookmark() {
        return InternalBookmark.from(session.lastBookmarks());
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return session.lastBookmarks();
    }

    // Temporary private API
    @Deprecated
    public void reset() {
        Futures.blockingGet(
                session.resetAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while resetting the session"));
    }

    <T> T execute(
            AccessMode accessMode,
            TransactionCallback<T> callback,
            TransactionConfig config,
            TelemetryApi telemetryApi,
            boolean flush) {
        return transaction(
                accessMode, tx -> callback.execute(new DelegatingTransactionContext(tx)), config, telemetryApi, flush);
    }

    private <T> T transaction(
            AccessMode mode,
            @SuppressWarnings("deprecation") TransactionWork<T> work,
            TransactionConfig config,
            TelemetryApi telemetryApi,
            boolean flush) {

        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        var apiTelemetryWork = new ApiTelemetryWork(telemetryApi);
        var span = GlobalOpenTelemetry.getTracer("Driver", "5.15").spanBuilder("Client Tx").startSpan();

            var r = session.retryLogic().retry(() -> {
                try (var ignored2 = span.makeCurrent()) {
                    try (var tx = beginTransaction(mode, config, apiTelemetryWork, flush)) {

                        var result = work.execute(tx);
                        if (result instanceof Result) {
                            throw new ClientException(String.format(
                                    "%s is not a valid return value, it should be consumed before producing a return value",
                                    Result.class.getName()));
                        }
                        if (tx.isOpen()) {
                            // commit tx if a user has not explicitly committed or rolled back the transaction
                            tx.commit();
                        }
                        return result;
                    }
                }
            });
        try (var ignored = span.makeCurrent()) {
            span.end();
        }
        return r;
    }

    private Transaction beginTransaction(
            AccessMode mode, TransactionConfig config, ApiTelemetryWork apiTelemetryWork, boolean flush) {
        var span = GlobalOpenTelemetry
                .getTracer("driver", "5.15.0")
                .spanBuilder("Transaction")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
            var tx = Futures.blockingGet(
                    session.beginTransactionAsync(mode, config, null, apiTelemetryWork, flush, span),
                    () -> terminateConnectionOnThreadInterrupt("Thread interrupted while starting a transaction"));
            return new InternalTransaction(tx, span);
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
