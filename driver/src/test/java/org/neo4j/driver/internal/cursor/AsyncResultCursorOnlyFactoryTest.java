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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.util.Futures.getNow;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import io.opentelemetry.api.trace.Span;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.AutoPullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

class AsyncResultCursorOnlyFactoryTest {
    // asyncResult
    @Test
    void shouldReturnAsyncResultWhenRunSucceeded() {
        // Given
        var connection = mock(Connection.class);
        ResultCursorFactory cursorFactory = newResultCursorFactory(connection, null);

        // When
        var cursorFuture = cursorFactory.asyncResult();

        // Then
        verifyRunCompleted(connection, cursorFuture);
    }

    @Test
    void shouldReturnAsyncResultWithRunErrorWhenRunFailed() {
        // Given
        Throwable error = new RuntimeException("Hi there");
        ResultCursorFactory cursorFactory = newResultCursorFactory(error);

        // When
        var cursorFuture = cursorFactory.asyncResult();

        // Then
        var cursor = getNow(cursorFuture);
        var actual = assertThrows(error.getClass(), () -> await(cursor.mapSuccessfulRunCompletionAsync()));
        assertSame(error, actual);
    }

    @Test
    void shouldPrePopulateRecords() {
        // Given
        var connection = mock(Connection.class);
        var runMessage = mock(Message.class);

        var runHandler = mock(RunResponseHandler.class);
        var runFuture = new CompletableFuture<Void>();

        var pullAllHandler = mock(PullAllResponseHandler.class);

        ResultCursorFactory cursorFactory =
                new AsyncResultCursorOnlyFactory(connection, runMessage, runHandler, runFuture, pullAllHandler, mock(Span.class));

        // When
        cursorFactory.asyncResult();

        // Then
        verify(pullAllHandler).prePopulateRecords();
    }

    // rxResult
    @Test
    void shouldErrorForRxResult() {
        // Given
        ResultCursorFactory cursorFactory = newResultCursorFactory(null);

        // When & Then
        var rxCursorFuture = cursorFactory.rxResult();
        var error = assertThrows(CompletionException.class, () -> getNow(rxCursorFuture));
        assertThat(
                error.getCause().getMessage(),
                containsString("Driver is connected to the database that does not support driver reactive API"));
    }

    private AsyncResultCursorOnlyFactory newResultCursorFactory(Connection connection, Throwable runError) {
        var runMessage = mock(Message.class);

        var runHandler = mock(RunResponseHandler.class);
        var runFuture = new CompletableFuture<Void>();
        if (runError != null) {
            runFuture.completeExceptionally(runError);
        } else {
            runFuture.complete(null);
        }

        var pullHandler = mock(AutoPullResponseHandler.class);

        return new AsyncResultCursorOnlyFactory(connection, runMessage, runHandler, runFuture, pullHandler, mock(Span.class));
    }

    private AsyncResultCursorOnlyFactory newResultCursorFactory(Throwable runError) {
        var connection = mock(Connection.class);
        return newResultCursorFactory(connection, runError);
    }

    private void verifyRunCompleted(Connection connection, CompletionStage<AsyncResultCursor> cursorFuture) {
        verify(connection).write(any(Message.class), any(RunResponseHandler.class));
        assertThat(getNow(cursorFuture), instanceOf(AsyncResultCursor.class));
    }
}
