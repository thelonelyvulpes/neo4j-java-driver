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
package org.neo4j.driver.internal.handlers.pulln;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.max;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.internal.messaging.request.DiscardMessage.newDiscardAllMessage;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Provides basic handling of pull responses from sever. The state is managed by {@link State}.
 */
public class BasicPullResponseHandler implements PullResponseHandler {
    private final Query query;
    protected final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;
    private final PullResponseCompletionListener completionListener;
    private final boolean syncSignals;
    protected final long maxRecordCount;
    protected AtomicLong counter = new AtomicLong(0);

    private State state;
    private long toRequest;
    private BiConsumer<Record, Throwable> recordConsumer = null;
    private BiConsumer<ResultSummary, Throwable> summaryConsumer = null;

    public BasicPullResponseHandler(
            Query query,
            RunResponseHandler runResponseHandler,
            Connection connection,
            MetadataExtractor metadataExtractor,
            PullResponseCompletionListener completionListener) {
        this(query, runResponseHandler, connection, metadataExtractor, completionListener, false, -1);
    }

    public BasicPullResponseHandler(
            Query query,
            RunResponseHandler runResponseHandler,
            Connection connection,
            MetadataExtractor metadataExtractor,
            PullResponseCompletionListener completionListener,
            boolean syncSignals,
            long maxRecordCount) {
        this.query = requireNonNull(query);
        this.runResponseHandler = requireNonNull(runResponseHandler);
        this.metadataExtractor = requireNonNull(metadataExtractor);
        this.connection = requireNonNull(connection);
        this.completionListener = requireNonNull(completionListener);
        this.syncSignals = syncSignals;

        this.state = State.READY_STATE;
        this.maxRecordCount = maxRecordCount;
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        State newState;
        BiConsumer<Record, Throwable> recordConsumer = null;
        BiConsumer<ResultSummary, Throwable> summaryConsumer = null;
        ResultSummary summary = null;
        Neo4jException exception = null;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onSuccess(this, metadata);
            newState = state;
            if (newState == State.SUCCEEDED_STATE) {
                completionListener.afterSuccess(metadata);
                try {
                    summary = extractResultSummary(metadata);
                } catch (Neo4jException e) {
                    summary = extractResultSummary(emptyMap());
                    exception = e;
                }
                recordConsumer = this.recordConsumer;
                summaryConsumer = this.summaryConsumer;
                if (syncSignals) {
                    complete(summaryConsumer, recordConsumer, summary, exception);
                }
                dispose();
            } else if (newState == State.READY_STATE) {
                if (toRequest > 0 || toRequest == UNLIMITED_FETCH_SIZE) {
                    request(toRequest);
                    toRequest = 0;
                }
                // summary consumer use (null, null) to identify done handling of success with has_more
                this.summaryConsumer.accept(null, null);
            }
        }
        if (!syncSignals && newState == State.SUCCEEDED_STATE) {
            complete(summaryConsumer, recordConsumer, summary, exception);
        }
    }

    @Override
    public void onFailure(Throwable error) {
        BiConsumer<Record, Throwable> recordConsumer;
        BiConsumer<ResultSummary, Throwable> summaryConsumer;
        ResultSummary summary;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onFailure(this, error);
            completionListener.afterFailure(error);
            summary = extractResultSummary(emptyMap());
            recordConsumer = this.recordConsumer;
            summaryConsumer = this.summaryConsumer;
            if (syncSignals) {
                complete(summaryConsumer, recordConsumer, summary, error);
            }
            dispose();
        }
        if (!syncSignals) {
            complete(summaryConsumer, recordConsumer, summary, error);
        }
    }

    @Override
    public void onRecord(Value[] fields) {
        State newState;
        Record record = null;
        synchronized (this) {
            assertRecordAndSummaryConsumerInstalled();
            state.onRecord(this, fields);
            newState = state;
            if (newState == State.STREAMING_STATE) {
                record = new InternalRecord(runResponseHandler.queryKeys(), fields);
                if (syncSignals) {
                    recordConsumer.accept(record, null);
                }
            }
        }
        if (!syncSignals && newState == State.STREAMING_STATE) {
            recordConsumer.accept(record, null);
        }
    }

    @Override
    public synchronized void request(long size) {
        assertRecordAndSummaryConsumerInstalled();
        state.request(this, size);
    }

    @Override
    public synchronized void cancel() {
        assertRecordAndSummaryConsumerInstalled();
        state.cancel(this);
    }

    protected void writePull(long n) {
        connection.writeAndFlush(new PullMessage(n, runResponseHandler.queryId()), this);
    }

    protected void discardAll() {
        connection.writeAndFlush(newDiscardAllMessage(runResponseHandler.queryId()), this);
    }

    @Override
    public synchronized void installSummaryConsumer(BiConsumer<ResultSummary, Throwable> summaryConsumer) {
        if (this.summaryConsumer != null) {
            throw new IllegalStateException("Summary consumer already installed.");
        }
        this.summaryConsumer = summaryConsumer;
    }

    @Override
    public synchronized void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer) {
        if (this.recordConsumer != null) {
            throw new IllegalStateException("Record consumer already installed.");
        }
        this.recordConsumer = recordConsumer;
    }

    protected boolean isDone() {
        return state.equals(State.SUCCEEDED_STATE) || state.equals(State.FAILURE_STATE);
    }

    private ResultSummary extractResultSummary(Map<String, Value> metadata) {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        return metadataExtractor.extractSummary(query, connection, resultAvailableAfter, metadata);
    }

    private void addToRequest(long toAdd) {
        if (toRequest == UNLIMITED_FETCH_SIZE) {
            return;
        }
        if (toAdd == UNLIMITED_FETCH_SIZE) {
            // pull all
            toRequest = UNLIMITED_FETCH_SIZE;
            return;
        }

        if (toAdd <= 0) {
            throw new IllegalArgumentException(
                    "Cannot request record amount that is less than or equal to 0. Request amount: " + toAdd);
        }
        toRequest += toAdd;
        if (toRequest <= 0) // toAdd is already at least 1, we hit buffer overflow
        {
            toRequest = Long.MAX_VALUE;
        }
    }

    private void assertRecordAndSummaryConsumerInstalled() {
        if (isDone()) {
            // no need to check if we've finished.
            return;
        }
        if (recordConsumer == null || summaryConsumer == null) {
            throw new IllegalStateException(format(
                    "Access record stream without record consumer and/or summary consumer. "
                            + "Record consumer=%s, Summary consumer=%s",
                    recordConsumer, summaryConsumer));
        }
    }

    private void complete(
            BiConsumer<ResultSummary, Throwable> summaryConsumer,
            BiConsumer<Record, Throwable> recordConsumer,
            ResultSummary summary,
            Throwable error) {
        // we first inform the summary consumer to ensure when streaming finished, summary is definitely available.
        summaryConsumer.accept(summary, error);
        // record consumer use (null, null) to identify the end of record stream
        recordConsumer.accept(null, error);
    }

    private void dispose() {
        // release the reference to the consumers who hold the reference to subscribers which shall be released when
        // subscription is completed.
        this.recordConsumer = null;
        this.summaryConsumer = null;
    }

    protected State state() {
        return state;
    }

    protected void state(State state) {
        this.state = state;
    }

    enum State {
        READY_STATE {
            @Override
            void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(BasicPullResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(BasicPullResponseHandler context, Value[] fields) {
                context.state(READY_STATE);
            }

            @Override
            void request(BasicPullResponseHandler context, long n) {
                context.state(STREAMING_STATE);
                context.writePull(n);
            }

            @Override
            void cancel(BasicPullResponseHandler context) {
                context.state(CANCELLED_STATE);
                context.discardAll();
            }
        },
        STREAMING_STATE {
            @Override
            void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata) {
                if (metadata.getOrDefault("has_more", BooleanValue.FALSE).asBoolean()) {
                    context.state(READY_STATE);
                } else {
                    context.state(SUCCEEDED_STATE);
                }
            }

            @Override
            void onFailure(BasicPullResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(BasicPullResponseHandler context, Value[] fields) {
                context.state(STREAMING_STATE);
            }

            @Override
            void request(BasicPullResponseHandler context, long n) {
                context.state(STREAMING_STATE);
                context.addToRequest(n);
            }

            @Override
            void cancel(BasicPullResponseHandler context) {
                context.state(CANCELLED_STATE);
            }
        },
        CANCELLED_STATE {
            @Override
            void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata) {
                if (metadata.getOrDefault("has_more", BooleanValue.FALSE).asBoolean()) {
                    context.state(CANCELLED_STATE);
                    context.discardAll();
                } else {
                    context.state(SUCCEEDED_STATE);
                }
            }

            @Override
            void onFailure(BasicPullResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(BasicPullResponseHandler context, Value[] fields) {
                context.state(CANCELLED_STATE);
            }

            @Override
            void request(BasicPullResponseHandler context, long n) {
                context.state(CANCELLED_STATE);
            }

            @Override
            void cancel(BasicPullResponseHandler context) {
                context.state(CANCELLED_STATE);
            }
        },
        SUCCEEDED_STATE {
            @Override
            void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(BasicPullResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(BasicPullResponseHandler context, Value[] fields) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void request(BasicPullResponseHandler context, long n) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void cancel(BasicPullResponseHandler context) {
                context.state(SUCCEEDED_STATE);
            }
        },
        FAILURE_STATE {
            @Override
            void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata) {
                context.state(SUCCEEDED_STATE);
            }

            @Override
            void onFailure(BasicPullResponseHandler context, Throwable error) {
                context.state(FAILURE_STATE);
            }

            @Override
            void onRecord(BasicPullResponseHandler context, Value[] fields) {
                context.state(FAILURE_STATE);
            }

            @Override
            void request(BasicPullResponseHandler context, long n) {
                context.state(FAILURE_STATE);
            }

            @Override
            void cancel(BasicPullResponseHandler context) {
                context.state(FAILURE_STATE);
            }
        };

        abstract void onSuccess(BasicPullResponseHandler context, Map<String, Value> metadata);

        abstract void onFailure(BasicPullResponseHandler context, Throwable error);

        abstract void onRecord(BasicPullResponseHandler context, Value[] fields);

        abstract void request(BasicPullResponseHandler context, long n);

        abstract void cancel(BasicPullResponseHandler context);
    }
}
