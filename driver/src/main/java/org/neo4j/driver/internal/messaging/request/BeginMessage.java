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

import static org.neo4j.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.internal.OtelEncodingUtils;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.value.MapValue;

public class BeginMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x11;

    public BeginMessage(
            Set<Bookmark> bookmarks,
            TransactionConfig config,
            DatabaseName databaseName,
            AccessMode mode,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging) {
        this(
                bookmarks,
                config.timeout(),
                config.metadata(),
                mode,
                databaseName,
                impersonatedUser,
                txType,
                notificationConfig,
                logging,
                Span.current());
    }
    public BeginMessage(
            Set<Bookmark> bookmarks,
            TransactionConfig config,
            DatabaseName databaseName,
            AccessMode mode,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging,
            Span span) {
        this(
                bookmarks,
                config.timeout(),
                config.metadata(),
                mode,
                databaseName,
                impersonatedUser,
                txType,
                notificationConfig,
                logging,
                span);
    }


    public BeginMessage(
            Set<Bookmark> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            AccessMode mode,
            DatabaseName databaseName,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            Logging logging,
            Span span) {
        super(buildMetadata(
                txTimeout,
                txMetadata,
                databaseName,
                mode,
                bookmarks,
                impersonatedUser,
                txType,
                notificationConfig,
                logging,
                Optional.of(span)));
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
        var that = (BeginMessage) o;
        return Objects.equals(metadata(), that.metadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata());
    }

    @Override
    public String toString() {
        return "BEGIN " + metadata();
    }

}
