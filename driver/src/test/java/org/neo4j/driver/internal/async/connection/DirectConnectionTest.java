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
package org.neo4j.driver.internal.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.internal.spi.Connection;

public class DirectConnectionTest {
    @Test
    void shouldReturnServerAgent() {
        // given
        var connection = mock(Connection.class);
        var directConnection = new DirectConnection(connection, defaultDatabase(), READ, null);
        var agent = "Neo4j/4.2.5";
        given(connection.serverAgent()).willReturn(agent);

        // when
        var actualAgent = directConnection.serverAgent();

        // then
        assertEquals(agent, actualAgent);
        then(connection).should().serverAgent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReturnTelemetryEnabledReturnNetworkValue(Boolean telemetryEnabled) {
        var connection = mock(Connection.class);
        doReturn(telemetryEnabled).when(connection).isTelemetryEnabled();

        var directConnection = new DirectConnection(connection, defaultDatabase(), READ, null);

        assertEquals(telemetryEnabled, directConnection.isTelemetryEnabled());
    }
}
