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
package org.neo4j.driver.internal.cluster;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.opentelemetry.api.trace.Span;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Futures;

public class RoutingTableHandlerImpl implements RoutingTableHandler {
    private final RoutingTable routingTable;
    private final DatabaseName databaseName;
    private final RoutingTableRegistry routingTableRegistry;
    private volatile CompletableFuture<RoutingTable> refreshRoutingTableFuture;
    private final ConnectionPool connectionPool;
    private final Rediscovery rediscovery;
    private final Logger log;
    private final long routingTablePurgeDelayMs;
    private final Set<BoltServerAddress> resolvedInitialRouters = new HashSet<>();

    public RoutingTableHandlerImpl(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            ConnectionPool connectionPool,
            RoutingTableRegistry routingTableRegistry,
            Logging logging,
            long routingTablePurgeDelayMs) {
        this.routingTable = routingTable;
        this.databaseName = routingTable.database();
        this.rediscovery = rediscovery;
        this.connectionPool = connectionPool;
        this.routingTableRegistry = routingTableRegistry;
        this.log = logging.getLog(getClass());
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
    }

    @Override
    public void onConnectionFailure(BoltServerAddress address) {
        // remove server from the routing table, to prevent concurrent threads from making connections to this address
        routingTable.forget(address);
    }

    @Override
    public void onWriteFailure(BoltServerAddress address) {
        routingTable.forgetWriter(address);
    }

    @Override
    public synchronized CompletionStage<RoutingTable> ensureRoutingTable(ConnectionContext context) {
        if (refreshRoutingTableFuture != null) {
            // refresh is already happening concurrently, just use it's result
            return refreshRoutingTableFuture;
        } else if (routingTable.isStaleFor(context.mode())) {
            // existing routing table is not fresh and should be updated
            log.debug("Routing table for database '%s' is stale. %s", databaseName.description(), routingTable);

            var resultFuture = new CompletableFuture<RoutingTable>();
            refreshRoutingTableFuture = resultFuture;

            rediscovery
                    .lookupClusterComposition(
                            routingTable,
                            connectionPool,
                            context.rediscoveryBookmarks(),
                            null,
                            context.overrideAuthToken(),
                            Span.current())
                    .whenComplete((composition, completionError) -> {
                        var error = Futures.completionExceptionCause(completionError);
                        if (error != null) {
                            clusterCompositionLookupFailed(error);
                        } else {
                            freshClusterCompositionFetched(composition);
                        }
                    });

            return resultFuture;
        } else {
            // existing routing table is fresh, use it
            return completedFuture(routingTable);
        }
    }

    @Override
    public synchronized CompletionStage<RoutingTable> updateRoutingTable(
            ClusterCompositionLookupResult compositionLookupResult) {
        if (refreshRoutingTableFuture != null) {
            // refresh is already happening concurrently, just use its result
            return refreshRoutingTableFuture;
        } else {
            if (compositionLookupResult.getClusterComposition().expirationTimestamp()
                    < routingTable.expirationTimestamp()) {
                return completedFuture(routingTable);
            }
            var resultFuture = new CompletableFuture<RoutingTable>();
            refreshRoutingTableFuture = resultFuture;
            freshClusterCompositionFetched(compositionLookupResult);
            return resultFuture;
        }
    }

    private synchronized void freshClusterCompositionFetched(ClusterCompositionLookupResult compositionLookupResult) {
        try {
            log.debug(
                    "Fetched cluster composition for database '%s'. %s",
                    databaseName.description(), compositionLookupResult.getClusterComposition());
            routingTable.update(compositionLookupResult.getClusterComposition());
            routingTableRegistry.removeAged();

            Set<BoltServerAddress> addressesToRetain = new LinkedHashSet<>();
            routingTableRegistry.allServers().stream()
                    .flatMap(BoltServerAddress::unicastStream)
                    .forEach(addressesToRetain::add);
            compositionLookupResult.getResolvedInitialRouters().ifPresent(addresses -> {
                resolvedInitialRouters.clear();
                resolvedInitialRouters.addAll(addresses);
            });
            addressesToRetain.addAll(resolvedInitialRouters);
            connectionPool.retainAll(addressesToRetain);

            log.debug("Updated routing table for database '%s'. %s", databaseName.description(), routingTable);

            var routingTableFuture = refreshRoutingTableFuture;
            refreshRoutingTableFuture = null;
            routingTableFuture.complete(routingTable);
        } catch (Throwable error) {
            clusterCompositionLookupFailed(error);
        }
    }

    private synchronized void clusterCompositionLookupFailed(Throwable error) {
        log.error(
                String.format(
                        "Failed to update routing table for database '%s'. Current routing table: %s.",
                        databaseName.description(), routingTable),
                error);
        routingTableRegistry.remove(databaseName);
        var routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.completeExceptionally(error);
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    @Override
    public Set<BoltServerAddress> servers() {
        return routingTable.servers();
    }

    // This method cannot be synchronized as it will be visited by all routing table handler's threads concurrently
    @Override
    public boolean isRoutingTableAged() {
        return refreshRoutingTableFuture == null && routingTable.hasBeenStaleFor(routingTablePurgeDelayMs);
    }

    public RoutingTable routingTable() {
        return routingTable;
    }
}
