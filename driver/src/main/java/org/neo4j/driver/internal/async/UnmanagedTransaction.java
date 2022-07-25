/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal.async;

import static org.neo4j.driver.internal.util.Futures.asCompletionException;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.futureCompletingConsumer;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;

public class UnmanagedTransaction {
    private enum State {
        /**
         * The transaction is running with no explicit success or failure marked
         */
        ACTIVE,

        /**
         * This transaction has been terminated either because of a fatal connection error.
         */
        TERMINATED,

        /**
         * This transaction has successfully committed
         */
        COMMITTED,

        /**
         * This transaction has been rolled back
         */
        ROLLED_BACK
    }

    protected static final String CANT_COMMIT_COMMITTED_MSG = "Can't commit, transaction has been committed";
    protected static final String CANT_ROLLBACK_COMMITTED_MSG = "Can't rollback, transaction has been committed";
    protected static final String CANT_COMMIT_ROLLED_BACK_MSG = "Can't commit, transaction has been rolled back";
    protected static final String CANT_ROLLBACK_ROLLED_BACK_MSG = "Can't rollback, transaction has been rolled back";
    protected static final String CANT_COMMIT_ROLLING_BACK_MSG =
            "Can't commit, transaction has been requested to be rolled back";
    protected static final String CANT_ROLLBACK_COMMITTING_MSG =
            "Can't rollback, transaction has been requested to be committed";
    private static final EnumSet<State> OPEN_STATES = EnumSet.of(State.ACTIVE, State.TERMINATED);

    private final Connection connection;
    private final BoltProtocol protocol;
    private final BookmarksHolder bookmarksHolder;
    private final ResultCursorsHolder resultCursors;
    private final long fetchSize;
    private final Lock lock = new ReentrantLock();
    private State state = State.ACTIVE;
    private CompletableFuture<Void> commitFuture;
    private CompletableFuture<Void> rollbackFuture;
    private Throwable causeOfTermination;
    private CompletionStage<Void> interruptStage;

    public UnmanagedTransaction(Connection connection, BookmarksHolder bookmarksHolder, long fetchSize) {
        this(connection, bookmarksHolder, fetchSize, new ResultCursorsHolder());
    }

    protected UnmanagedTransaction(
            Connection connection, BookmarksHolder bookmarksHolder, long fetchSize, ResultCursorsHolder resultCursors) {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarksHolder = bookmarksHolder;
        this.resultCursors = resultCursors;
        this.fetchSize = fetchSize;
    }

    public CompletionStage<UnmanagedTransaction> beginAsync(Set<Bookmark> initialBookmarks, TransactionConfig config) {
        return protocol.beginTransaction(connection, initialBookmarks, config).handle((ignore, beginError) -> {
            if (beginError != null) {
                if (beginError instanceof AuthorizationExpiredException) {
                    connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
                } else if (beginError instanceof ConnectionReadTimeoutException) {
                    connection.terminateAndRelease(beginError.getMessage());
                } else {
                    connection.release();
                }
                throw asCompletionException(beginError);
            }
            return this;
        });
    }

    public CompletionStage<Void> closeAsync() {
        return closeAsync(false);
    }

    public CompletionStage<Void> closeAsync(boolean commit) {
        return closeAsync(commit, true);
    }

    public CompletionStage<Void> commitAsync() {
        return closeAsync(true, false);
    }

    public CompletionStage<Void> rollbackAsync() {
        return closeAsync(false, false);
    }

    public CompletionStage<ResultCursor> runAsync(Query query) {
        return runAsync(query, -1);
    }

    public CompletionStage<ResultCursor> runAsync(Query query, long maxRecordCount) {
        ensureCanRunQueries();
        CompletionStage<AsyncResultCursor> cursorStage = protocol.runInUnmanagedTransaction(
                        connection, query, this, fetchSize, maxRecordCount)
                .asyncResult();
        resultCursors.add(cursorStage);
        return cursorStage
                .thenCompose(AsyncResultCursor::mapSuccessfulRunCompletionAsync)
                .thenApply(cursor -> cursor);
    }

    public CompletionStage<RxResultCursor> runRx(Query query) {
        ensureCanRunQueries();
        CompletionStage<RxResultCursor> cursorStage = protocol.runInUnmanagedTransaction(
                        connection, query, this, fetchSize)
                .rxResult();
        resultCursors.add(cursorStage);
        return cursorStage;
    }

    public boolean isOpen() {
        return OPEN_STATES.contains(executeWithLock(lock, () -> state));
    }

    public void markTerminated(Throwable cause) {
        executeWithLock(lock, () -> {
            if (state == State.TERMINATED) {
                if (causeOfTermination != null) {
                    addSuppressedWhenNotCaptured(causeOfTermination, cause);
                }
            } else {
                state = State.TERMINATED;
                causeOfTermination = cause;
            }
        });
    }

    private void addSuppressedWhenNotCaptured(Throwable currentCause, Throwable newCause) {
        if (currentCause != newCause) {
            boolean noneMatch =
                    Arrays.stream(currentCause.getSuppressed()).noneMatch(suppressed -> suppressed == newCause);
            if (noneMatch) {
                currentCause.addSuppressed(newCause);
            }
        }
    }

    public Connection connection() {
        return connection;
    }

    private void ensureCanRunQueries() {
        executeWithLock(lock, () -> {
            if (state == State.COMMITTED) {
                throw new ClientException("Cannot run more queries in this transaction, it has been committed");
            } else if (state == State.ROLLED_BACK) {
                throw new ClientException("Cannot run more queries in this transaction, it has been rolled back");
            } else if (state == State.TERMINATED) {
                throw new ClientException(
                        "Cannot run more queries in this transaction, "
                                + "it has either experienced an fatal error or was explicitly terminated",
                        causeOfTermination);
            }
        });
    }

    private CompletionStage<Void> doCommitAsync(Throwable cursorFailure) {
        ClientException exception = executeWithLock(
                lock,
                () -> state == State.TERMINATED
                        ? new ClientException(
                                "Transaction can't be committed. "
                                        + "It has been rolled back either because of an error or explicit termination",
                                cursorFailure != causeOfTermination ? causeOfTermination : null)
                        : null);
        return exception != null
                ? failedFuture(exception)
                : protocol.commitTransaction(connection).thenAccept(bookmarksHolder::setBookmark);
    }

    private CompletionStage<Void> doRollbackAsync() {
        return executeWithLock(lock, () -> state) == State.TERMINATED
                ? completedWithNull()
                : protocol.rollbackTransaction(connection);
    }

    private static BiFunction<Void, Throwable, Void> handleCommitOrRollback(Throwable cursorFailure) {
        return (ignore, commitOrRollbackError) -> {
            CompletionException combinedError = combineErrors(cursorFailure, commitOrRollbackError);
            if (combinedError != null) {
                throw combinedError;
            }
            return null;
        };
    }

    private void handleTransactionCompletion(boolean commitAttempt, Throwable throwable) {
        executeWithLock(lock, () -> {
            if (commitAttempt && throwable == null) {
                state = State.COMMITTED;
            } else {
                state = State.ROLLED_BACK;
            }
        });
        if (throwable instanceof AuthorizationExpiredException) {
            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        } else if (throwable instanceof ConnectionReadTimeoutException) {
            connection.terminateAndRelease(throwable.getMessage());
        } else {
            connection.release(); // release in background
        }
    }

    private CompletionStage<Void> closeAsync(boolean commit, boolean completeWithNullIfNotOpen) {
        CompletionStage<Void> stage = executeWithLock(lock, () -> {
            CompletionStage<Void> resultStage = null;
            if (completeWithNullIfNotOpen && !isOpen()) {
                resultStage = completedWithNull();
            } else if (state == State.COMMITTED) {
                resultStage = failedFuture(
                        new ClientException(commit ? CANT_COMMIT_COMMITTED_MSG : CANT_ROLLBACK_COMMITTED_MSG));
            } else if (state == State.ROLLED_BACK) {
                resultStage = failedFuture(
                        new ClientException(commit ? CANT_COMMIT_ROLLED_BACK_MSG : CANT_ROLLBACK_ROLLED_BACK_MSG));
            } else {
                if (commit) {
                    if (rollbackFuture != null) {
                        resultStage = failedFuture(new ClientException(CANT_COMMIT_ROLLING_BACK_MSG));
                    } else if (commitFuture != null) {
                        resultStage = commitFuture;
                    } else {
                        commitFuture = new CompletableFuture<>();
                    }
                } else {
                    if (commitFuture != null) {
                        resultStage = failedFuture(new ClientException(CANT_ROLLBACK_COMMITTING_MSG));
                    } else if (rollbackFuture != null) {
                        resultStage = rollbackFuture;
                    } else {
                        rollbackFuture = new CompletableFuture<>();
                    }
                }
            }
            return resultStage;
        });

        if (stage == null) {
            CompletableFuture<Void> targetFuture;
            Function<Throwable, CompletionStage<Void>> targetAction;
            if (commit) {
                targetFuture = commitFuture;
                targetAction = throwable -> doCommitAsync(throwable).handle(handleCommitOrRollback(throwable));
            } else {
                targetFuture = rollbackFuture;
                targetAction = throwable -> doRollbackAsync().handle(handleCommitOrRollback(throwable));
            }
            resultCursors
                    .retrieveNotConsumedError()
                    .thenCompose(targetAction)
                    .whenComplete((ignored, throwable) -> handleTransactionCompletion(commit, throwable))
                    .whenComplete(futureCompletingConsumer(targetFuture));
            stage = targetFuture;
        }

        return stage;
    }

    /**
     * Marks transaction as terminated and sends {@code RESET} message over allocated connection.
     * <p>
     * <b>THIS METHOD IS NOT PART OF PUBLIC API. This method may be changed or removed at any moment in time.</b>
     *
     * @return {@code RESET} response stage
     */
    public CompletionStage<Void> interruptAsync() {
        return executeWithLock(lock, () -> {
            if (interruptStage == null) {
                markTerminated(null);
                interruptStage = connection.reset();
            }
            return interruptStage;
        });
    }
}
