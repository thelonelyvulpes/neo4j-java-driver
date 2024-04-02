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
package org.neo4j.driver.internal.cluster;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import io.opentelemetry.api.trace.Span;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DomainNameResolver;
import org.neo4j.driver.internal.ImpersonationUtil;
import org.neo4j.driver.internal.ResolvedBoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

public class RediscoveryImpl implements Rediscovery {
    private static final String NO_ROUTERS_AVAILABLE =
            "Could not perform discovery for database '%s'. No routing server available.";
    private static final String RECOVERABLE_ROUTING_ERROR = "Failed to update routing table with server '%s'.";
    private static final String RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER =
            "Received a recoverable discovery error with server '%s', "
                    + "will continue discovery with other routing servers if available. "
                    + "Complete failure is reported separately from this entry.";
    private static final String TRANSACTION_INVALID_BOOKMARK_CODE = "Neo.ClientError.Transaction.InvalidBookmark";
    private static final String TRANSACTION_INVALID_BOOKMARK_MIXTURE_CODE =
            "Neo.ClientError.Transaction.InvalidBookmarkMixture";
    private static final String STATEMENT_ARGUMENT_ERROR_CODE = "Neo.ClientError.Statement.ArgumentError";
    private static final String REQUEST_INVALID_CODE = "Neo.ClientError.Request.Invalid";
    private static final String STATEMENT_TYPE_ERROR_CODE = "Neo.ClientError.Statement.TypeError";

    private final BoltServerAddress initialRouter;
    private final Logger log;
    private final ClusterCompositionProvider provider;
    private final ServerAddressResolver resolver;
    private final DomainNameResolver domainNameResolver;

    public RediscoveryImpl(
            BoltServerAddress initialRouter,
            ClusterCompositionProvider provider,
            ServerAddressResolver resolver,
            Logging logging,
            DomainNameResolver domainNameResolver) {
        this.initialRouter = initialRouter;
        this.log = logging.getLog(getClass());
        this.provider = provider;
        this.resolver = resolver;
        this.domainNameResolver = requireNonNull(domainNameResolver);
    }

    /**
     * Given a database and its current routing table, and the global connection pool, use the global cluster composition provider to fetch a new cluster
     * composition, which would be used to update the routing table of the given database and global connection pool.
     *
     * @param routingTable   current routing table of the given database.
     * @param connectionPool connection pool.
     * @return new cluster composition and an optional set of resolved initial router addresses.
     */
    @Override
    public CompletionStage<ClusterCompositionLookupResult> lookupClusterComposition(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Span span) {
        var result = new CompletableFuture<ClusterCompositionLookupResult>();
        // if we failed discovery, we will chain all errors into this one.
        var baseError = new ServiceUnavailableException(
                String.format(NO_ROUTERS_AVAILABLE, routingTable.database().description()));
        lookupClusterComposition(
                routingTable, connectionPool, result, bookmarks, impersonatedUser, overrideAuthToken, baseError, span);
        return result;
    }

    private void lookupClusterComposition(
            RoutingTable routingTable,
            ConnectionPool pool,
            CompletableFuture<ClusterCompositionLookupResult> result,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Throwable baseError,
            Span span) {
        lookup(routingTable, pool, bookmarks, impersonatedUser, overrideAuthToken, baseError, span)
                .whenComplete((compositionLookupResult, completionError) -> {
                    var error = Futures.completionExceptionCause(completionError);
                    if (error != null) {
                        result.completeExceptionally(error);
                    } else if (compositionLookupResult != null) {
                        result.complete(compositionLookupResult);
                    } else {
                        result.completeExceptionally(baseError);
                    }
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookup(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Throwable baseError,
            Span span) {
        CompletionStage<ClusterCompositionLookupResult> compositionStage;

        if (routingTable.preferInitialRouter()) {
            compositionStage = lookupOnInitialRouterThenOnKnownRouters(
                    routingTable, connectionPool, bookmarks, impersonatedUser, overrideAuthToken, baseError, span);
        } else {
            compositionStage = lookupOnKnownRoutersThenOnInitialRouter(
                    routingTable, connectionPool, bookmarks, impersonatedUser, overrideAuthToken, baseError, span);
        }

        return compositionStage;
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRoutersThenOnInitialRouter(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken authToken,
            Throwable baseError,
            Span span) {
        Set<BoltServerAddress> seenServers = new HashSet<>();
        return lookupOnKnownRouters(
                        routingTable, connectionPool, seenServers, bookmarks, impersonatedUser, authToken, baseError, span)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnInitialRouter(
                            routingTable,
                            connectionPool,
                            seenServers,
                            bookmarks,
                            impersonatedUser,
                            authToken,
                            baseError,
                            span);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouterThenOnKnownRouters(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Throwable baseError,
            Span span) {
        Set<BoltServerAddress> seenServers = emptySet();
        return lookupOnInitialRouter(
                        routingTable,
                        connectionPool,
                        seenServers,
                        bookmarks,
                        impersonatedUser,
                        overrideAuthToken,
                        baseError,
                        span)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnKnownRouters(
                            routingTable,
                            connectionPool,
                            new HashSet<>(),
                            bookmarks,
                            impersonatedUser,
                            overrideAuthToken,
                            baseError,
                            span);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRouters(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<BoltServerAddress> seenServers,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken authToken,
            Throwable baseError,
            Span span) {
        CompletableFuture<ClusterComposition> result = completedWithNull();
        for (var address : routingTable.routers()) {
            result = result.thenCompose(composition -> {
                if (composition != null) {
                    return completedFuture(composition);
                } else {
                    return lookupOnRouter(
                            address,
                            true,
                            routingTable,
                            connectionPool,
                            seenServers,
                            bookmarks,
                            impersonatedUser,
                            authToken,
                            baseError,
                            span);
                }
            });
        }
        return result.thenApply(
                composition -> composition != null ? new ClusterCompositionLookupResult(composition) : null);
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouter(
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<BoltServerAddress> seenServers,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Throwable baseError,
            Span span) {
        List<BoltServerAddress> resolvedRouters;
        try {
            resolvedRouters = resolve();
        } catch (Throwable error) {
            return failedFuture(error);
        }
        Set<BoltServerAddress> resolvedRouterSet = new HashSet<>(resolvedRouters);
        resolvedRouters.removeAll(seenServers);

        CompletableFuture<ClusterComposition> result = completedWithNull();
        for (var address : resolvedRouters) {
            result = result.thenCompose(composition -> {
                if (composition != null) {
                    return completedFuture(composition);
                }
                return lookupOnRouter(
                        address,
                        false,
                        routingTable,
                        connectionPool,
                        null,
                        bookmarks,
                        impersonatedUser,
                        overrideAuthToken,
                        baseError,
                        span);
            });
        }
        return result.thenApply(composition ->
                composition != null ? new   ClusterCompositionLookupResult(composition, resolvedRouterSet) : null);
    }

    private CompletionStage<ClusterComposition> lookupOnRouter(
            BoltServerAddress routerAddress,
            boolean resolveAddress,
            RoutingTable routingTable,
            ConnectionPool connectionPool,
            Set<BoltServerAddress> seenServers,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            AuthToken overrideAuthToken,
            Throwable baseError,
            Span span) {
        var addressFuture = CompletableFuture.completedFuture(routerAddress);

        return addressFuture
                .thenApply(address ->
                        resolveAddress ? resolveByDomainNameOrThrowCompletionException(address, routingTable) : address)
                .thenApply(address -> addAndReturn(seenServers, address))
                .thenCompose(address -> {
                    try (var scope = span.makeCurrent()) {
                        return connectionPool.acquire(address, overrideAuthToken);
                    }
                })
                .thenApply(connection -> ImpersonationUtil.ensureImpersonationSupport(connection, impersonatedUser))
                .thenCompose(connection -> provider.getClusterComposition(
                        connection, routingTable.database(), bookmarks, impersonatedUser))
                .handle((response, error) -> {
                    var cause = Futures.completionExceptionCause(error);
                    if (cause != null) {
                        return handleRoutingProcedureError(cause, routingTable, routerAddress, baseError);
                    } else {
                        return response;
                    }
                });
    }

    @SuppressWarnings({"ThrowableNotThrown", "SameReturnValue"})
    private ClusterComposition handleRoutingProcedureError(
            Throwable error, RoutingTable routingTable, BoltServerAddress routerAddress, Throwable baseError) {
        if (mustAbortDiscovery(error)) {
            throw new CompletionException(error);
        }

        // Retryable error happened during discovery.
        var discoveryError = new DiscoveryException(format(RECOVERABLE_ROUTING_ERROR, routerAddress), error);
        Futures.combineErrors(baseError, discoveryError); // we record each failure here
        log.warn(RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER, routerAddress);
        log.debug(format(RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER, routerAddress), discoveryError);
        routingTable.forget(routerAddress);
        return null;
    }

    private boolean mustAbortDiscovery(Throwable throwable) {
        var abort = false;

        if (!(throwable instanceof AuthorizationExpiredException) && throwable instanceof SecurityException) {
            abort = true;
        } else if (throwable instanceof FatalDiscoveryException) {
            abort = true;
        } else if (throwable instanceof IllegalStateException
                && ConnectionPool.CONNECTION_POOL_CLOSED_ERROR_MESSAGE.equals(throwable.getMessage())) {
            abort = true;
        } else if (throwable instanceof AuthTokenManagerExecutionException) {
            abort = true;
        } else if (throwable instanceof UnsupportedFeatureException) {
            abort = true;
        } else if (throwable instanceof ClientException) {
            var code = ((ClientException) throwable).code();
            abort = switch (code) {
                case TRANSACTION_INVALID_BOOKMARK_CODE,
                        TRANSACTION_INVALID_BOOKMARK_MIXTURE_CODE,
                        STATEMENT_ARGUMENT_ERROR_CODE,
                        REQUEST_INVALID_CODE,
                        STATEMENT_TYPE_ERROR_CODE -> true;
                default -> false;};
        }

        return abort;
    }

    @Override
    public List<BoltServerAddress> resolve() throws UnknownHostException {
        List<BoltServerAddress> resolvedAddresses = new LinkedList<>();
        UnknownHostException exception = null;
        for (var serverAddress : resolver.resolve(initialRouter)) {
            try {
                resolveAllByDomainName(serverAddress).unicastStream().forEach(resolvedAddresses::add);
            } catch (UnknownHostException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        // give up only if there are no addresses to work with at all
        if (resolvedAddresses.isEmpty() && exception != null) {
            throw exception;
        }

        return resolvedAddresses;
    }

    private <T> T addAndReturn(Collection<T> collection, T element) {
        if (collection != null) {
            collection.add(element);
        }
        return element;
    }

    private BoltServerAddress resolveByDomainNameOrThrowCompletionException(
            BoltServerAddress address, RoutingTable routingTable) {
        try {
            var resolvedAddress = resolveAllByDomainName(address);
            routingTable.replaceRouterIfPresent(address, resolvedAddress);
            return resolvedAddress
                    .unicastStream()
                    .findFirst()
                    .orElseThrow(
                            () -> new IllegalStateException(
                                    "Unexpected condition, the ResolvedBoltServerAddress must always have at least one unicast address"));
        } catch (Throwable e) {
            throw new CompletionException(e);
        }
    }

    private ResolvedBoltServerAddress resolveAllByDomainName(ServerAddress address) throws UnknownHostException {
        return new ResolvedBoltServerAddress(
                address.host(), address.port(), domainNameResolver.resolve(address.host()));
    }
}
