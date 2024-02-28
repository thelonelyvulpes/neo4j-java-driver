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
package org.neo4j.driver.testutil;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.opentelemetry.api.OpenTelemetry;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.NoOpBookmarkManager;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.messaging.v52.BoltProtocolV52;
import org.neo4j.driver.internal.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.messaging.v54.BoltProtocolV54;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class TestUtil {
    public static final BoltProtocolVersion DEFAULT_TEST_PROTOCOL_VERSION = BoltProtocolV4.VERSION;
    public static final BoltProtocol DEFAULT_TEST_PROTOCOL = BoltProtocol.forVersion(DEFAULT_TEST_PROTOCOL_VERSION);

    private static final long DEFAULT_WAIT_TIME_MS = MINUTES.toMillis(2);
    private static final String ALPHANUMERICS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789";
    public static final Duration TX_TIMEOUT_TEST_TIMEOUT = Duration.ofSeconds(10);

    private TestUtil() {}

    public static <T> List<T> await(Publisher<T> publisher) {
        return await(Flux.from(publisher));
    }

    public static <T> T await(Mono<T> publisher) {
        EventLoopGroupFactory.assertNotInEventLoopThread();
        return publisher.block(Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public static <T> List<T> await(Flux<T> publisher) {
        EventLoopGroupFactory.assertNotInEventLoopThread();
        return publisher.collectList().block(Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public static <T> List<T> awaitAll(List<CompletionStage<T>> stages) {
        return stages.stream().map(TestUtil::await).collect(toList());
    }

    public static <T> T await(CompletionStage<T> stage) {
        return await((Future<T>) stage.toCompletableFuture());
    }

    public static <T> T await(CompletableFuture<T> future) {
        return await((Future<T>) future);
    }

    public static <T, U extends Future<T>> T await(U future) {
        try {
            return future.get(DEFAULT_WAIT_TIME_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future: " + future, e);
        } catch (ExecutionException e) {
            PlatformDependent.throwException(e.getCause());
            return null;
        } catch (TimeoutException e) {
            throw new AssertionError("Given future did not complete in time: " + future);
        }
    }

    public static void assertByteBufContains(ByteBuf buf, Number... values) {
        try {
            assertNotNull(buf);
            var expectedReadableBytes =
                    Arrays.stream(values).mapToInt(TestUtil::bytesCount).sum();
            assertEquals(expectedReadableBytes, buf.readableBytes(), "Unexpected number of bytes");
            for (var expectedValue : values) {
                var actualValue = read(buf, expectedValue.getClass());
                var valueType = actualValue.getClass().getSimpleName();
                assertEquals(expectedValue, actualValue, valueType + " values not equal");
            }
        } finally {
            releaseIfPossible(buf);
        }
    }

    public static void assertByteBufEquals(ByteBuf expected, ByteBuf actual) {
        try {
            assertEquals(expected, actual);
        } finally {
            releaseIfPossible(expected);
            releaseIfPossible(actual);
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asOrderedSet(T... elements) {
        return new LinkedHashSet<>(Arrays.asList(elements));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asSet(T... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    public static void cleanDb(Driver driver) {
        try (var session = driver.session()) {
            cleanDb(session);
        }
    }

    public static void dropDatabase(Driver driver, String database) {
        var databaseExists = databaseExists(driver, database);
        if (!databaseExists) {
            return;
        }

        try (var session = driver.session(forDatabase("system"))) {
            session.run("DROP DATABASE " + database).consume();
        }
    }

    public static void createDatabase(Driver driver, String database) {
        var databaseExists = databaseExists(driver, database);
        if (databaseExists) {
            return;
        }

        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run("CREATE DATABASE " + database).consume();
        }
    }

    public static boolean databaseExists(Driver driver, String database) {
        try (var session = driver.session(forDatabase("system"))) {
            // No procedure equivalent and `call dbms.database.state("db")` also throws an exception when db doesn't
            // exist
            return session.run("SHOW DATABASES").stream()
                    .anyMatch(r -> r.get("name").asString().equals(database));
        }
    }

    public static NetworkSession newSession(ConnectionProvider connectionProvider, Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, WRITE, bookmarks);
    }

    private static NetworkSession newSession(
            ConnectionProvider connectionProvider, AccessMode mode, Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, mode, new FixedRetryLogic(0), bookmarks);
    }

    public static NetworkSession newSession(ConnectionProvider connectionProvider, AccessMode mode) {
        return newSession(connectionProvider, mode, Collections.emptySet());
    }

    public static NetworkSession newSession(ConnectionProvider connectionProvider, RetryLogic logic) {
        return newSession(connectionProvider, WRITE, logic, Collections.emptySet());
    }

    public static NetworkSession newSession(ConnectionProvider connectionProvider) {
        return newSession(connectionProvider, WRITE, Collections.emptySet());
    }

    public static NetworkSession newSession(
            ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic, Set<Bookmark> bookmarks) {
        return newSession(connectionProvider, mode, retryLogic, bookmarks, true);
    }

    public static NetworkSession newSession(
            ConnectionProvider connectionProvider,
            AccessMode mode,
            RetryLogic retryLogic,
            Set<Bookmark> bookmarks,
            boolean telemetryDisabled) {
        return new NetworkSession(
                connectionProvider,
                retryLogic,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                UNLIMITED_FETCH_SIZE,
                DEV_NULL_LOGGING,
                NoOpBookmarkManager.INSTANCE,
                null,
                null,
                telemetryDisabled,
                OpenTelemetry.noop());
    }

    public static void verifyRunRx(Connection connection, String query) {
        verify(connection).writeAndFlush(argThat(runWithMetaMessageWithQueryMatcher(query)), any());
    }

    public static void verifyRunAndPull(Connection connection, String query) {
        verify(connection).write(argThat(runWithMetaMessageWithQueryMatcher(query)), any());
        verify(connection).writeAndFlush(any(PullMessage.class), any());
    }

    public static void verifyCommitTx(Connection connection, VerificationMode mode) {
        verify(connection, mode).writeAndFlush(any(CommitMessage.class), any());
    }

    public static void verifyCommitTx(Connection connection) {
        verifyCommitTx(connection, times(1));
    }

    public static void verifyRollbackTx(Connection connection, VerificationMode mode) {
        verify(connection, mode).writeAndFlush(any(RollbackMessage.class), any());
    }

    public static void verifyRollbackTx(Connection connection) {
        verifyRollbackTx(connection, times(1));
    }

    public static void verifyBeginTx(Connection connectionMock) {
        verifyBeginTx(connectionMock, 1);
    }

    public static void verifyBeginTx(Connection connectionMock, int times) {
        verify(connectionMock, times(times)).writeAndFlush(any(BeginMessage.class), any(BeginTxResponseHandler.class));
    }

    public static void setupFailingRun(Connection connection, Throwable error) {
        doAnswer(invocation -> {
                    ResponseHandler runHandler = invocation.getArgument(1);
                    runHandler.onFailure(error);
                    return null;
                })
                .when(connection)
                .write(any(RunWithMetadataMessage.class), any());

        doAnswer(invocation -> {
                    ResponseHandler pullHandler = invocation.getArgument(1);
                    pullHandler.onFailure(error);
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(PullMessage.class), any());
    }

    public static void setupFailingBegin(Connection connection, Throwable error) {
        // with bookmarks
        doAnswer(invocation -> {
                    ResponseHandler handler = invocation.getArgument(1);
                    handler.onFailure(error);
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(BeginMessage.class), any(BeginTxResponseHandler.class));
    }

    public static void setupFailingCommit(Connection connection) {
        setupFailingCommit(connection, 1);
    }

    public static void setupFailingCommit(Connection connection, int times) {
        doAnswer(new Answer<Void>() {
                    int invoked;

                    @Override
                    public Void answer(InvocationOnMock invocation) {
                        ResponseHandler handler = invocation.getArgument(1);
                        if (invoked++ < times) {
                            handler.onFailure(new ServiceUnavailableException(""));
                        } else {
                            handler.onSuccess(emptyMap());
                        }
                        return null;
                    }
                })
                .when(connection)
                .writeAndFlush(any(CommitMessage.class), any());
    }

    public static void setupFailingRollback(Connection connection) {
        setupFailingRollback(connection, 1);
    }

    public static void setupFailingRollback(Connection connection, int times) {
        doAnswer(new Answer<Void>() {
                    int invoked;

                    @Override
                    public Void answer(InvocationOnMock invocation) {
                        ResponseHandler handler = invocation.getArgument(1);
                        if (invoked++ < times) {
                            handler.onFailure(new ServiceUnavailableException(""));
                        } else {
                            handler.onSuccess(emptyMap());
                        }
                        return null;
                    }
                })
                .when(connection)
                .writeAndFlush(any(RollbackMessage.class), any());
    }

    public static void setupSuccessfulRunAndPull(Connection connection) {
        doAnswer(invocation -> {
                    ResponseHandler runHandler = invocation.getArgument(1);
                    runHandler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .write(any(RunWithMetadataMessage.class), any());

        doAnswer(invocation -> {
                    ResponseHandler pullHandler = invocation.getArgument(1);
                    pullHandler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(PullMessage.class), any());
    }

    public static void setupSuccessfulRunRx(Connection connection) {
        doAnswer(invocation -> {
                    ResponseHandler runHandler = invocation.getArgument(1);
                    runHandler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(RunWithMetadataMessage.class), any());
    }

    public static void setupSuccessfulRunAndPull(Connection connection, String query) {
        doAnswer(invocation -> {
                    ResponseHandler runHandler = invocation.getArgument(1);
                    runHandler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .write(argThat(runWithMetaMessageWithQueryMatcher(query)), any());

        doAnswer(invocation -> {
                    ResponseHandler pullHandler = invocation.getArgument(1);
                    pullHandler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(PullMessage.class), any());
    }

    public static Connection connectionMock() {
        return connectionMock(BoltProtocolV42.INSTANCE);
    }

    public static Connection connectionMock(BoltProtocol protocol) {
        return connectionMock(WRITE, protocol);
    }

    public static Connection connectionMock(AccessMode mode, BoltProtocol protocol) {
        return connectionMock(null, mode, protocol);
    }

    public static Connection connectionMock(String databaseName, BoltProtocol protocol) {
        return connectionMock(databaseName, WRITE, protocol);
    }

    public static Connection connectionMock(String databaseName, AccessMode mode, BoltProtocol protocol) {
        var connection = mock(Connection.class);
        when(connection.serverAddress()).thenReturn(BoltServerAddress.LOCAL_DEFAULT);
        when(connection.protocol()).thenReturn(protocol);
        when(connection.mode()).thenReturn(mode);
        when(connection.databaseName()).thenReturn(database(databaseName));
        var version = protocol.version();
        if (List.of(
                        BoltProtocolV3.VERSION,
                        BoltProtocolV4.VERSION,
                        BoltProtocolV41.VERSION,
                        BoltProtocolV42.VERSION,
                        BoltProtocolV43.VERSION,
                        BoltProtocolV44.VERSION,
                        BoltProtocolV5.VERSION,
                        BoltProtocolV51.VERSION,
                        BoltProtocolV52.VERSION,
                        BoltProtocolV53.VERSION,
                        BoltProtocolV54.VERSION)
                .contains(version)) {
            setupSuccessResponse(connection, CommitMessage.class);
            setupSuccessResponse(connection, RollbackMessage.class);
            setupSuccessResponse(connection, BeginMessage.class);
            when(connection.release()).thenReturn(completedWithNull());
            when(connection.reset(any())).thenReturn(completedWithNull());
        } else {
            throw new IllegalArgumentException("Unsupported bolt protocol version: " + version);
        }
        return connection;
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void interruptWhenInWaitingState(Thread thread) {
        CompletableFuture.runAsync(() -> {
            // spin until given thread moves to WAITING state
            do {
                sleep(500);
            } while (thread.getState() != Thread.State.WAITING);

            thread.interrupt();
        });
    }

    public static String randomString(int size) {
        var random = ThreadLocalRandom.current();
        return IntStream.range(0, size)
                .mapToObj(i -> String.valueOf(ALPHANUMERICS.charAt(random.nextInt(ALPHANUMERICS.length()))))
                .collect(Collectors.joining());
    }

    public static ArgumentMatcher<Message> runWithMetaMessageWithQueryMatcher(String query) {
        return message -> message instanceof RunWithMetadataMessage
                && Objects.equals(query, ((RunWithMetadataMessage) message).query());
    }

    public static ArgumentMatcher<Message> beginMessage() {
        return beginMessageWithPredicate(ignored -> true);
    }

    public static ArgumentMatcher<Message> beginMessageWithPredicate(Predicate<BeginMessage> predicate) {
        return message -> message instanceof BeginMessage && predicate.test((BeginMessage) message);
    }

    public static void assertNoCircularReferences(Throwable ex) {
        assertNoCircularReferences(ex, new ArrayList<>());
    }

    private static void assertNoCircularReferences(Throwable ex, List<Throwable> list) {
        list.add(ex);
        if (ex.getCause() != null) {
            if (list.contains(ex.getCause())) {
                throw new AssertionError("Circular reference detected", ex.getCause());
            }
            assertNoCircularReferences(ex.getCause(), list);
        }
        for (var suppressed : ex.getSuppressed()) {
            if (list.contains(suppressed)) {
                throw new AssertionError("Circular reference detected", suppressed);
            }
            assertNoCircularReferences(suppressed, list);
        }
    }

    private static void setupSuccessResponse(Connection connection, Class<? extends Message> messageType) {
        doAnswer(invocation -> {
                    ResponseHandler handler = invocation.getArgument(1);
                    handler.onSuccess(emptyMap());
                    return null;
                })
                .when(connection)
                .writeAndFlush(any(messageType), any());
    }

    private static void cleanDb(Session session) {
        int nodesDeleted;
        do {
            nodesDeleted = deleteBatchOfNodes(session);
        } while (nodesDeleted > 0);
    }

    private static int deleteBatchOfNodes(Session session) {
        return session.executeWrite(tx -> {
            var result = tx.run("MATCH (n) WITH n LIMIT 1000 DETACH DELETE n RETURN count(n)");
            return result.single().get(0).asInt();
        });
    }

    private static Number read(ByteBuf buf, Class<? extends Number> type) {
        if (type == Byte.class) {
            return buf.readByte();
        } else if (type == Short.class) {
            return buf.readShort();
        } else if (type == Integer.class) {
            return buf.readInt();
        } else if (type == Long.class) {
            return buf.readLong();
        } else if (type == Float.class) {
            return buf.readFloat();
        } else if (type == Double.class) {
            return buf.readDouble();
        } else {
            throw new IllegalArgumentException("Unexpected numeric type: " + type);
        }
    }

    private static int bytesCount(Number value) {
        if (value instanceof Byte) {
            return 1;
        } else if (value instanceof Short) {
            return 2;
        } else if (value instanceof Integer) {
            return 4;
        } else if (value instanceof Long) {
            return 8;
        } else if (value instanceof Float) {
            return 4;
        } else if (value instanceof Double) {
            return 8;
        } else {
            throw new IllegalArgumentException("Unexpected number: '" + value + "' or type" + value.getClass());
        }
    }

    private static void releaseIfPossible(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release();
        }
    }

    public static <T extends Serializable> T serializeAndReadBack(T instance, Class<T> targetClass)
            throws IOException, ClassNotFoundException {

        var bos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(instance);
        }
        bos.close();

        try (var oos = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return targetClass.cast(oos.readObject());
        }
    }
}
