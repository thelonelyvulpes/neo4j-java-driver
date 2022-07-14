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

import static org.neo4j.driver.internal.async.ConnectionContext.PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.supportsMultiDatabase;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;

/**
 * Simple {@link ConnectionProvider connection provider} that obtains connections form the given pool only for the given address.
 */
public class DirectConnectionProvider implements ConnectionProvider {
    private final BoltServerAddress address;
    private final ConnectionPool connectionPool;

    DirectConnectionProvider(BoltServerAddress address, ConnectionPool connectionPool) {
        this.address = address;
        this.connectionPool = connectionPool;
    }

    @Override
    public CompletionStage<Connection> acquireConnection(ConnectionContext context) {
        CompletableFuture<DatabaseName> databaseNameFuture = context.databaseNameFuture();
        databaseNameFuture.complete(DatabaseNameUtil.defaultDatabase());
        return acquireConnection()
                .thenApply(connection ->
                        new DirectConnection(
                        connection,
                        Futures.joinNowOrElseThrow(databaseNameFuture, PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER),
                        context.mode(),
                        context.impersonatedUser()));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return acquireConnection().thenCompose(Connection::release);
    }

    @Override
    public CompletionStage<Void> close() {
        return connectionPool.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return acquireConnection().thenCompose(conn -> {
            boolean supportsMultiDatabase = supportsMultiDatabase(conn);
            return conn.release().thenApply(ignored -> supportsMultiDatabase);
        });
    }

    @Override
    public CompletionStage<Boolean> supportsAutoQueryRouting() {
        return acquireConnection().thenCompose(conn -> {
            var supportsAuto = conn.supportsAutoRoutedQuery();
            return conn.release().thenApply((_x) -> supportsAuto);
        });
    }

    public BoltServerAddress getAddress() {
        return address;
    }

    /**
     * Used only for grabbing a connection with the server after hello message.
     * This connection cannot be directly used for running any queries as it is missing necessary connection context
     */
    private CompletionStage<Connection> acquireConnection() {
        return connectionPool.acquire(address);
    }
}
