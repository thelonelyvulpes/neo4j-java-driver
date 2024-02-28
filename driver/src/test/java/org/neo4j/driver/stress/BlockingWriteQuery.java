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
import org.neo4j.driver.summary.ResultSummary;

public class BlockingWriteQuery<C extends AbstractContext> extends AbstractBlockingQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    BlockingWriteQuery(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void execute(C context) {
        ResultSummary summary = null;
        Throwable queryError = null;

        try (var session = newSession(AccessMode.WRITE, context)) {
            summary = session.run("CREATE ()").consume();
            context.setBookmark(session.lastBookmark());
        } catch (Throwable error) {
            queryError = error;
            if (!stressTest.handleWriteFailure(error, context)) {
                throw error;
            }
        }

        if (queryError == null && summary != null) {
            assertEquals(1, summary.counters().nodesCreated());
            context.nodeCreated();
        }
    }
}
