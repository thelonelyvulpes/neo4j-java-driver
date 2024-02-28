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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RxFailingQuery<C extends AbstractContext> extends AbstractRxQuery<C> {
    public RxFailingQuery(Driver driver) {
        super(driver, false);
    }

    @Override
    @SuppressWarnings("deprecation")
    public CompletionStage<Void> execute(C context) {
        var queryFinished = new CompletableFuture<Void>();
        Flux.usingWhen(
                        Mono.fromSupplier(() -> newSession(AccessMode.READ, context)),
                        session -> session.run("UNWIND [10, 5, 0] AS x RETURN 10 / x")
                                .records(),
                        RxSession::close)
                .subscribe(
                        record -> assertThat(
                                record.get(0).asInt(), either(equalTo(1)).or(equalTo(2))),
                        error -> {
                            var cause = Futures.completionExceptionCause(error);
                            assertThat(cause, is(arithmeticError()));
                            queryFinished.complete(null);
                        });
        return queryFinished;
    }
}
