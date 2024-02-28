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
package org.neo4j.driver.internal.spi;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.async.ConnectionContext;

/**
 * Interface defines a layer used by the driver to obtain connections. It is meant to be the only component that
 * differs between "direct" and "routing" driver.
 */
public interface ConnectionProvider {
    CompletionStage<Connection> acquireConnection(ConnectionContext context);

    /**
     * The validation of connectivity will happen with the default database.
     */
    CompletionStage<Void> verifyConnectivity();

    CompletionStage<Void> close();

    CompletionStage<Boolean> supportsMultiDb();

    CompletionStage<Boolean> supportsSessionAuth();
}
