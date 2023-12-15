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
package org.neo4j.driver.internal.messaging.request;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

import java.time.Duration;
import java.util.*;

import io.opentelemetry.api.internal.TemporaryBuffers;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;

public class RunWithMetadataMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x10;

    private final String query;
    private final Map<String, Value> parameters;

    public static RunWithMetadataMessage autoCommitTxRunMessage(
            Query query,
            TransactionConfig config,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            NotificationConfig notificationConfig,
            Logging logging,
            Span span) {
        return autoCommitTxRunMessage(
                query,
                config.timeout(),
                config.metadata(),
                databaseName,
                mode,
                bookmarks,
                impersonatedUser,
                notificationConfig,
                logging,
                span);
    }

    public static RunWithMetadataMessage autoCommitTxRunMessage(
            Query query,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            NotificationConfig notificationConfig,
            Logging logging,
            Span span) {
        var metadata = buildMetadata(
                txTimeout,
                txMetadata,
                databaseName,
                mode,
                bookmarks,
                impersonatedUser,
                null,
                notificationConfig,
                logging,
                Optional.of(span));
        return new RunWithMetadataMessage(query.text(), query.parameters().asMap(ofValue()), metadata);
    }

    public static RunWithMetadataMessage unmanagedTxRunMessage(Query query, Span span) {
        return new RunWithMetadataMessage(query.text(), query.parameters().asMap(ofValue()), applyCtxW3(span, new HashMap<>(2)));
    }

    static Map<String, Value> applyCtxW3(Span x, Map<String, Value> result) {
        var spanContext = x.getSpanContext();
        var TRACE_ID_HEX_SIZE = TraceId.getLength();
        var SPAN_ID_HEX_SIZE = SpanId.getLength();
        var TRACE_OPTION_HEX_SIZE = TraceFlags.getLength();
        var SPAN_ID_OFFSET = 3 + TRACE_ID_HEX_SIZE + 1;
        var TRACE_OPTION_OFFSET = SPAN_ID_OFFSET + SPAN_ID_HEX_SIZE + 1;
        var TRACEPARENT_HEADER_SIZE = TRACE_OPTION_OFFSET + TRACE_OPTION_HEX_SIZE;
        char[] chars = TemporaryBuffers.chars(TRACEPARENT_HEADER_SIZE);
        chars[0] = "00".charAt(0);
        chars[1] = "00".charAt(1);
        chars[2] = '-';
        String traceId = spanContext.getTraceId();
        traceId.getChars(0, traceId.length(), chars, 3);
        chars[SPAN_ID_OFFSET - 1] = '-';
        String spanId = spanContext.getSpanId();
        spanId.getChars(0, spanId.length(), chars, SPAN_ID_OFFSET);
        chars[TRACE_OPTION_OFFSET - 1] = '-';
        String traceFlagsHex = spanContext.getTraceFlags().asHex();
        chars[TRACE_OPTION_OFFSET] = traceFlagsHex.charAt(0);
        chars[TRACE_OPTION_OFFSET + 1] = traceFlagsHex.charAt(1);

        result.put("traceparent", value(new String(chars, 0, TRACEPARENT_HEADER_SIZE)));
        TraceState traceState = spanContext.getTraceState();
        result.put("tracestate", value(W3CTraceContextEncoding.encodeTraceState(traceState)));
        return result;
    }

    private RunWithMetadataMessage(String query, Map<String, Value> parameters, Map<String, Value> metadata) {
        super(metadata);
        this.query = query;
        this.parameters = parameters;
    }

    public String query() {
        return query;
    }

    public Map<String, Value> parameters() {
        return parameters;
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (RunWithMetadataMessage) o;
        return Objects.equals(query, that.query)
                && Objects.equals(parameters, that.parameters)
                && Objects.equals(metadata(), that.metadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, parameters, metadata());
    }

    @Override
    public String toString() {
        return "RUN \"" + query + "\" " + parameters + " " + metadata();
    }
}
