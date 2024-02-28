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
package org.neo4j.driver.internal.messaging.v54;

import static java.time.Duration.ofSeconds;
import static java.util.Calendar.DECEMBER;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RouteMessage;
import org.neo4j.driver.internal.messaging.request.TelemetryMessage;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.util.messaging.AbstractMessageWriterTestBase;

public class MessageWriterV54Test extends AbstractMessageWriterTestBase {
    @Override
    protected MessageFormat.Writer newWriter(PackOutput output) {
        return BoltProtocolV54.INSTANCE.createMessageFormat().newWriter(output);
    }

    @Override
    protected Stream<Message> supportedMessages() {
        return Stream.of(
                // Bolt V2 Data Types
                unmanagedTxRunMessage(new Query("RETURN $point", singletonMap("point", point(42, 12.99, -180.0))), null),
                unmanagedTxRunMessage(
                        new Query("RETURN $point", singletonMap("point", point(42, 0.51, 2.99, 100.123))), null),
                unmanagedTxRunMessage(
                        new Query("RETURN $date", singletonMap("date", value(LocalDate.ofEpochDay(2147483650L)))), null),
                unmanagedTxRunMessage(new Query(
                        "RETURN $time", singletonMap("time", value(OffsetTime.of(4, 16, 20, 999, ZoneOffset.MIN)))), null),
                unmanagedTxRunMessage(
                        new Query("RETURN $time", singletonMap("time", value(LocalTime.of(12, 9, 18, 999_888)))), null),
                unmanagedTxRunMessage(new Query(
                        "RETURN $dateTime",
                        singletonMap("dateTime", value(LocalDateTime.of(2049, DECEMBER, 12, 17, 25, 49, 199)))), null),
                unmanagedTxRunMessage(new Query(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                value(ZonedDateTime.of(
                                        2000, 1, 10, 12, 2, 49, 300, ZoneOffset.ofHoursMinutes(9, 30))))), null),
                unmanagedTxRunMessage(new Query(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                value(ZonedDateTime.of(2000, 1, 10, 12, 2, 49, 300, ZoneId.of("Europe/Stockholm"))))), null),

                // New Bolt V4 messages
                new PullMessage(100, 200),
                new DiscardMessage(300, 400),

                // Bolt V3 messages
                new HelloMessage(
                        "MyDriver/1.2.3",
                        BoltAgentUtil.VALUE,
                        ((InternalAuthToken) basic("neo4j", "neo4j")).toMap(),
                        Collections.emptyMap(),
                        false,
                        null),
                GOODBYE,
                new BeginMessage(
                        Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx123")),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        READ,
                        defaultDatabase(),
                        null,
                        null,
                        null,
                        Logging.none(),
                        null),
                new BeginMessage(
                        Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx123")),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        WRITE,
                        database("foo"),
                        null,
                        null,
                        null,
                        Logging.none(),
                        null),
                COMMIT,
                ROLLBACK,
                RESET,
                autoCommitTxRunMessage(
                        new Query("RETURN 1"),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        defaultDatabase(),
                        READ,
                        Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx1")),
                        null,
                        null,
                        Logging.none(),
                        null),
                autoCommitTxRunMessage(
                        new Query("RETURN 1"),
                        ofSeconds(5),
                        singletonMap("key", value(42)),
                        database("foo"),
                        WRITE,
                        Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx1")),
                        null,
                        null,
                        Logging.none(),
                        null),
                unmanagedTxRunMessage(new Query("RETURN 1"), null),

                // Bolt V3 messages with struct values
                autoCommitTxRunMessage(
                        new Query("RETURN $x", singletonMap("x", value(ZonedDateTime.now()))),
                        ofSeconds(1),
                        emptyMap(),
                        defaultDatabase(),
                        READ,
                        Collections.emptySet(),
                        null,
                        null,
                        Logging.none(),
                        null),
                autoCommitTxRunMessage(
                        new Query("RETURN $x", singletonMap("x", value(ZonedDateTime.now()))),
                        ofSeconds(1),
                        emptyMap(),
                        database("foo"),
                        WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        Logging.none(),
                        null),
                unmanagedTxRunMessage(new Query("RETURN $x", singletonMap("x", point(42, 1, 2, 3))), null),

                // New 4.3 Messages
                routeMessage(),
                // New 5.4 message
                telemetryMessage());
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(
                // Bolt V1, V2 and V3 messages
                PULL_ALL, DISCARD_ALL);
    }

    private RouteMessage routeMessage() {
        Map<String, Value> routeContext = new HashMap<>();
        routeContext.put("someContext", Values.value(124));
        return new RouteMessage(routeContext, Collections.emptySet(), "dbName", null);
    }

    private TelemetryMessage telemetryMessage() {
        return new TelemetryMessage(1);
    }
}
