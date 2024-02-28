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
package org.neo4j.driver.internal.async.pool;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.Collections;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DefaultDomainNameResolver;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.metrics.DevNullMetricsListener;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class ConnectionPoolImplIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private ConnectionPoolImpl pool;

    @BeforeEach
    void setUp() {
        pool = newPool();
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void shouldAcquireConnectionWhenPoolIsEmpty() {
        var connection = await(pool.acquire(neo4j.address(), null));

        assertNotNull(connection);
    }

    @Test
    void shouldAcquireIdleConnection() {
        var connection1 = await(pool.acquire(neo4j.address(), null));
        await(connection1.release());

        var connection2 = await(pool.acquire(neo4j.address(), null));
        assertNotNull(connection2);
    }

    @Test
    void shouldBeAbleToClosePoolInIOWorkerThread() {
        // In the IO worker thread of a channel obtained from a pool, we shall be able to close the pool.
        var future = pool.acquire(neo4j.address(), null)
                .thenCompose(Connection::release)
                // This shall close all pools
                .whenComplete((ignored, error) -> pool.retainAll(Collections.emptySet()));

        // We should be able to come to this line.
        await(future);
    }

    @Test
    void shouldFailToAcquireConnectionToWrongAddress() {
        var e = assertThrows(
                ServiceUnavailableException.class,
                () -> await(pool.acquire(new BoltServerAddress("wrong-localhost"), null)));

        assertThat(e.getMessage(), startsWith("Unable to connect"));
    }

    @Test
    void shouldFailToAcquireWhenPoolClosed() {
        var connection = await(pool.acquire(neo4j.address(), null));
        await(connection.release());
        await(pool.close());

        var e = assertThrows(IllegalStateException.class, () -> pool.acquire(neo4j.address(), null));
        assertThat(e.getMessage(), startsWith("Pool closed"));
    }

    @Test
    void shouldNotCloseWhenClosed() {
        assertNull(await(pool.close()));
        assertTrue(pool.close().toCompletableFuture().isDone());
    }

    @Test
    void shouldFailToAcquireConnectionWhenPoolIsClosed() {
        await(pool.acquire(neo4j.address(), null));
        var channelPool = this.pool.getPool(neo4j.address());
        await(channelPool.close());
        var error = assertThrows(ServiceUnavailableException.class, () -> await(pool.acquire(neo4j.address(), null)));
        assertThat(error.getMessage(), containsString("closed while acquiring a connection"));
        assertThat(error.getCause(), instanceOf(IllegalStateException.class));
        assertThat(error.getCause().getMessage(), containsString("FixedChannelPool was closed"));
    }

    private ConnectionPoolImpl newPool() {
        var clock = new FakeClock();
        var connectionSettings = new ConnectionSettings(neo4j.authTokenManager(), "test", 5000);
        ChannelConnector connector = new ChannelConnectorImpl(
                connectionSettings,
                SecurityPlanImpl.insecure(),
                DEV_NULL_LOGGING,
                clock,
                RoutingContext.EMPTY,
                DefaultDomainNameResolver.getInstance(),
                null,
                BoltAgentUtil.VALUE);
        var poolSettings = newSettings();
        var bootstrap = BootstrapFactory.newBootstrap(1);
        return new ConnectionPoolImpl(
                connector, bootstrap, poolSettings, DevNullMetricsListener.INSTANCE, DEV_NULL_LOGGING, clock, true);
    }

    private static PoolSettings newSettings() {
        return new PoolSettings(10, 5000, -1, -1, OpenTelemetry.noop());
    }
}
