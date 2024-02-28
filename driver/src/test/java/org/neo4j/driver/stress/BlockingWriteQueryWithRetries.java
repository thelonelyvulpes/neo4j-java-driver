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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;

public class BlockingWriteQueryWithRetries<C extends AbstractContext> extends AbstractBlockingQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    BlockingWriteQueryWithRetries(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void execute(C context) {
        try (var session = newSession(AccessMode.WRITE, context)) {
            var resultSummary =
                    session.writeTransaction(tx -> tx.run("CREATE ()").consume());
            assertEquals(1, resultSummary.counters().nodesCreated());
            context.nodeCreated();
            context.setBookmark(session.lastBookmark());
        } catch (RuntimeException error) {
            stressTest.handleWriteFailure(error, context);
            throw error;
        }
    }
}
