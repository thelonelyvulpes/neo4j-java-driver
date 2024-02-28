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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RxWriteQueryWithRetries<C extends AbstractContext> extends AbstractRxQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    RxWriteQueryWithRetries(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    @SuppressWarnings("deprecation")
    public CompletionStage<Void> execute(C context) {
        var queryFinished = new CompletableFuture<Void>();

        var createdNodesNum = new AtomicInteger();
        Flux.usingWhen(
                        Mono.fromSupplier(driver::rxSession),
                        session -> session.writeTransaction(
                                tx -> tx.run("CREATE ()").consume()),
                        session -> Mono.empty(),
                        (session, error) -> session.close(),
                        RxSession::close)
                .subscribe(
                        resultSummary -> createdNodesNum.addAndGet(
                                resultSummary.counters().nodesCreated()),
                        error -> handleError(Futures.completionExceptionCause(error), context, queryFinished),
                        () -> {
                            assertEquals(1, createdNodesNum.get());
                            context.nodeCreated();
                            queryFinished.complete(null);
                        });

        return queryFinished;
    }

    private void handleError(Throwable error, C context, CompletableFuture<Void> queryFinished) {
        stressTest.handleWriteFailure(error, context);
        queryFinished.completeExceptionally(error);
    }
}
