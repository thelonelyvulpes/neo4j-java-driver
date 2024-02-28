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
package org.neo4j.driver.stress;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;

public class BlockingWriteQueryUsingReadSessionWithRetries<C extends AbstractContext> extends AbstractBlockingQuery<C> {
    public BlockingWriteQueryUsingReadSessionWithRetries(Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void execute(C context) {
        try (var session = newSession(AccessMode.READ, context)) {
            assertThrows(ClientException.class, () -> session.readTransaction(tx -> tx.run("CREATE ()")));
        }
    }
}
