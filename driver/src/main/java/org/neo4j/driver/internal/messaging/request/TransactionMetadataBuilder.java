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
package org.neo4j.driver.internal.messaging.request;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.Values.value;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.internal.TemporaryBuffers;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.StringValue;

public class TransactionMetadataBuilder {
    private static final String BOOKMARKS_METADATA_KEY = "bookmarks";
    private static final String DATABASE_NAME_KEY = "db";
    private static final String TX_TIMEOUT_METADATA_KEY = "tx_timeout";
    private static final String TX_METADATA_METADATA_KEY = "tx_metadata";
    private static final String MODE_KEY = "mode";
    private static final String MODE_READ_VALUE = "r";
    private static final String IMPERSONATED_USER_KEY = "imp_user";
    private static final String TX_TYPE_KEY = "tx_type";

    public static Map<String, Value> buildMetadata(
            Duration txTimeout,
            Map<String, Value> txMetadata,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging,
            Optional<Span> span) {
        var bookmarksPresent = !bookmarks.isEmpty();
        var txTimeoutPresent = txTimeout != null;
        var txMetadataPresent = txMetadata != null && !txMetadata.isEmpty();
        var accessModePresent = mode == AccessMode.READ;
        var databaseNamePresent = databaseName.databaseName().isPresent();
        var impersonatedUserPresent = impersonatedUser != null;
        var txTypePresent = txType != null;
        var notificationConfigPresent = notificationConfig != null;
        if (!bookmarksPresent
                && !txTimeoutPresent
                && !txMetadataPresent
                && !accessModePresent
                && !databaseNamePresent
                && !impersonatedUserPresent
                && !txTypePresent
                && !notificationConfigPresent) {
            return emptyMap();
        }

        Map<String, Value> result = Iterables.newHashMapWithSize(5);

        if (bookmarksPresent) {
            result.put(BOOKMARKS_METADATA_KEY, value(bookmarks.stream().map(Bookmark::value)));
        }
        if (txTimeoutPresent) {
            var millis = txTimeout.toMillis();
            if (txTimeout.toNanosPart() % 1_000_000 > 0) {
                var log = logging.getLog(TransactionMetadataBuilder.class);
                millis++;
                log.info(
                        "The transaction timeout has been rounded up to next millisecond value since the config had a fractional millisecond value");
            }
            result.put(TX_TIMEOUT_METADATA_KEY, value(millis));
        }
        if (txMetadataPresent) {
            result.put(TX_METADATA_METADATA_KEY, value(txMetadata));
        }
        if (accessModePresent) {
            result.put(MODE_KEY, value(MODE_READ_VALUE));
        }
        if (impersonatedUserPresent) {
            result.put(IMPERSONATED_USER_KEY, value(impersonatedUser));
        }
        if (txTypePresent) {
            result.put(TX_TYPE_KEY, value(txType));
        }
        MessageWithMetadata.appendNotificationConfig(result, notificationConfig);

        databaseName.databaseName().ifPresent(name -> result.put(DATABASE_NAME_KEY, value(name)));

        span.ifPresent(x -> applyCtxW3(x, result));

        return result;
    }

    static void applyCtxW3(Span x, Map<String, Value> result) {
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
    }


    void applyCtx(Span x, Map<String, Value> result) {
            System.out.println("extracting");
            try (var xScope = x.makeCurrent()) {
                GlobalOpenTelemetry
                        .getPropagators()
                        .getTextMapPropagator()
                        .inject(Context.current(), result, (f, s, s1) -> {
                            f.put(s, value(s1));
                        });
            }
    }
}
