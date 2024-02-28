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
package org.neo4j.driver.internal.cursor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.ResultSummary;

public class AsyncResultCursorImpl implements AsyncResultCursor {
    private final Throwable runError;
    private final RunResponseHandler runHandler;
    private final PullAllResponseHandler pullAllHandler;

    public AsyncResultCursorImpl(
            Throwable runError, RunResponseHandler runHandler, PullAllResponseHandler pullAllHandler) {
        this.runError = runError;
        this.runHandler = runHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public List<String> keys() {
        return runHandler.queryKeys().keys();
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync() {
        return pullAllHandler.consumeAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync() {
        return pullAllHandler.nextAsync();
    }

    @Override
    public CompletionStage<Record> peekAsync() {
        return pullAllHandler.peekAsync();
    }

    @Override
    public CompletionStage<Record> singleAsync() {
        return nextAsync().thenCompose(firstRecord -> {
            if (firstRecord == null) {
                throw new NoSuchRecordException("Cannot retrieve a single record, because this result is empty.");
            }
            return nextAsync().thenApply(secondRecord -> {
                if (secondRecord != null) {
                    throw new NoSuchRecordException("Expected a result with a single record, but this result "
                            + "contains at least one more. Ensure your query returns only "
                            + "one record.");
                }
                return firstRecord;
            });
        });
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action) {
        var resultFuture = new CompletableFuture<Void>();
        internalForEachAsync(action, resultFuture);
        return resultFuture.thenCompose(ignore -> consumeAsync());
    }

    @Override
    public CompletionStage<List<Record>> listAsync() {
        return listAsync(Function.identity());
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction) {
        return pullAllHandler.listAsync(mapFunction);
    }

    @Override
    public CompletionStage<Boolean> isOpenAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Throwable> discardAllFailureAsync() {
        // runError has priority over other errors and is expected to have been reported to user by now
        return consumeAsync().handle((summary, error) -> runError != null ? null : error);
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        // runError has priority over other errors and is expected to have been reported to user by now
        return pullAllHandler.pullAllFailureAsync().thenApply(error -> runError != null ? null : error);
    }

    private void internalForEachAsync(Consumer<Record> action, CompletableFuture<Void> resultFuture) {
        var recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync((record, completionError) -> {
            var error = Futures.completionExceptionCause(completionError);
            if (error != null) {
                resultFuture.completeExceptionally(error);
            } else if (record != null) {
                try {
                    action.accept(record);
                } catch (Throwable actionError) {
                    resultFuture.completeExceptionally(actionError);
                    return;
                }
                internalForEachAsync(action, resultFuture);
            } else {
                resultFuture.complete(null);
            }
        });
    }

    @Override
    public CompletableFuture<AsyncResultCursor> mapSuccessfulRunCompletionAsync() {
        return runError != null ? Futures.failedFuture(runError) : CompletableFuture.completedFuture(this);
    }
}
