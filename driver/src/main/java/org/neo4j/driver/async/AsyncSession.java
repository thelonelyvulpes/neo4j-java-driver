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
package org.neo4j.driver.async;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Values;

/**
 * Provides a context of work for database interactions.
 * <p>
 * An <em>AsyncSession</em> hosts a series of {@linkplain AsyncTransaction transactions}
 * carried out against a database. Within the database, all queries are
 * carried out within a transaction. Within application code, however, it is
 * not always necessary to explicitly {@link #beginTransactionAsync() begin a
 * transaction}. If a query is {@link #runAsync} directly against a {@link
 * AsyncSession}, the server will automatically <code>BEGIN</code> and
 * <code>COMMIT</code> that query within its own transaction. This type
 * of transaction is known as an <em>autocommit transaction</em>.
 * <p>
 * Unmanaged transactions allow multiple queries to be committed as part of
 * a single atomic operation and can be rolled back if necessary. They can also
 * be used to ensure <em>causal consistency</em>, meaning that an application
 * can run a series of queries on different members of a cluster, while
 * ensuring that each query sees the state of graph at least as up-to-date as
 * the graph seen by the previous query. For more on causal consistency, see
 * the Neo4j clustering manual.
 * <p>
 * Typically, a session will acquire a TCP connection to execute query or
 * transaction. Such a connection will be acquired from a connection pool
 * and released back there when query result is consumed or transaction is
 * committed or rolled back. One connection can therefore be adopted by many
 * sessions, although by only one at a time. Application code should never need
 * to deal directly with connection management.
 * <p>
 * A session inherits its destination address and permissions from its
 * underlying connection. This means that for a single query/transaction one
 * session may only ever target one machine within a cluster and does not
 * support re-authentication. To achieve otherwise requires creation of a
 * separate session.
 * <p>
 * Similarly, multiple sessions should be used when working with concurrency;
 * session implementations are not thread safe.
 *
 * @since 4.0
 */
public interface AsyncSession extends BaseSession, AsyncQueryRunner {
    /**
     * Begin a new <em>unmanaged {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with a new
     * {@link Transaction} object when begin operation is successful.
     * It is completed exceptionally if transaction can't be started.
     * <p>
     * Returned stage can be completed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous begin of a transaction.
     */
    CompletionStage<AsyncTransaction> beginTransactionAsync();

    /**
     * Begin a new <em>unmanaged {@linkplain AsyncTransaction transaction}</em> with the specified {@link TransactionConfig configuration}.
     * At most one transaction may exist in a session at any point in time.
     * To maintain multiple concurrent transactions, use multiple concurrent sessions.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with a new
     * {@link AsyncTransaction} object when begin operation is successful. It is completed exceptionally if
     * transaction can't be started.
     * <p>
     * Returned stage can be completed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param config configuration for the new transaction.
     * @return a {@link CompletionStage completion stage} that represents the asynchronous begin of a transaction.
     */
    CompletionStage<AsyncTransaction> beginTransactionAsync(TransactionConfig config);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#READ read} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new read transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     * @deprecated superseded by {@link #executeReadAsync(AsyncTransactionCallback)}.
     */
    @Deprecated
    <T> CompletionStage<T> readTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one
     * or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a completion stage that completes successfully with the result of the unit of work on success or completes exceptionally otherwise.
     */
    default <T> CompletionStage<T> executeReadAsync(AsyncTransactionCallback<CompletionStage<T>> callback) {
        return executeReadAsync(callback, TransactionConfig.empty());
    }

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#READ read} asynchronous transaction with
     * the specified {@link TransactionConfig configuration}.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link  AsyncTransactionWork} to be applied to a new read transaction. Operation executed by the
     * given work must be asynchronous.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     * @deprecated superseded by {@link #executeReadAsync(AsyncTransactionCallback, TransactionConfig)}.
     */
    @Deprecated
    <T> CompletionStage<T> readTransactionAsync(
            AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work
     * will result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     *
     * @param callback the callback representing the unit of work.
     * @param config   configuration for all transactions started to execute the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a completion stage that completes successfully with the result of the unit of work on success or completes exceptionally otherwise.
     */
    <T> CompletionStage<T> executeReadAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config);

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#WRITE write} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link  AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new write transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     * @deprecated superseded by {@link #executeWriteAsync(AsyncTransactionCallback)}.
     */
    @Deprecated
    <T> CompletionStage<T> writeTransactionAsync(AsyncTransactionWork<CompletionStage<T>> work);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for
     * one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a completion stage that completes successfully with the result of the unit of work on success or completes exceptionally otherwise.
     */
    default <T> CompletionStage<T> executeWriteAsync(AsyncTransactionCallback<CompletionStage<T>> callback) {
        return executeWriteAsync(callback, TransactionConfig.empty());
    }

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#WRITE write} asynchronous transaction with
     * the specified {@link TransactionConfig configuration}.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link AsyncTransaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link AsyncTransaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link AsyncTransactionWork} can be completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned stage and do not use them inside the
     * {@link AsyncTransactionWork}.
     * Consider using asynchronous calls throughout the chain or offloading blocking operation to a different {@link Executor}.
     * This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link AsyncTransactionWork} to be applied to a new write transaction. Operation executed by the
     * given work must be asynchronous.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     * @deprecated superseded by {@link #executeWriteAsync(AsyncTransactionCallback, TransactionConfig)}.
     */
    @Deprecated
    <T> CompletionStage<T> writeTransactionAsync(
            AsyncTransactionWork<CompletionStage<T>> work, TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work
     * will result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     *
     * @param callback the callback representing the unit of work.
     * @param config   configuration for all transactions started to execute the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a completion stage that completes successfully with the result of the unit of work on success or completes exceptionally otherwise.
     */
    <T> CompletionStage<T> executeWriteAsync(
            AsyncTransactionCallback<CompletionStage<T>> callback, TransactionConfig config);

    /**
     * Run a query asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncQueryRunner} for
     * more information.
     *
     * @param query text of a Neo4j query.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query, TransactionConfig config);

    /**
     * Run a query asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     * <h4>Example</h4>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * CompletionStage<ResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = $myNameParam RETURN (n)",
     *             parameters,
     *             config);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncQueryRunner} for
     * more information.
     *
     * @param query text of a Neo4j query.
     * @param parameters input data for the query.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query, Map<String, Object> parameters, TransactionConfig config);

    /**
     * Run a query asynchronously in an auto-commit transaction with the specified {@link TransactionConfig configuration} and return a
     * {@link CompletionStage} with a result cursor.
     * <h4>Example</h4>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Query query = new Query( "MATCH (n) WHERE n.name = $myNameParam RETURN n.age" );
     * CompletionStage<ResultCursor> cursorStage = session.runAsync(query, config);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc in {@link AsyncQueryRunner} for
     * more information.
     *
     * @param query a Neo4j query.
     * @param config configuration for the new transaction.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(Query query, TransactionConfig config);

    /**
     * Return the last bookmark of this session.
     * <p>
     * When no new bookmark is received, the initial bookmarks are returned as a composite {@link Bookmark} containing all initial bookmarks. This may happen
     * when no work has been done using the session. If no initial bookmarks have been provided, an empty {@link Bookmark} is returned.
     *
     * @return the last bookmark.
     */
    @Deprecated
    Bookmark lastBookmark();

    /**
     * Return a set of last bookmarks.
     * <p>
     * When no new bookmark is received, the initial bookmarks are returned. This may happen when no work has been done using the session. Multivalued {@link
     * Bookmark} instances will be mapped to distinct {@link Bookmark} instances. If no initial bookmarks have been provided, an empty set is returned.
     *
     * @return the immutable set of last bookmarks.
     */
    Set<Bookmark> lastBookmarks();

    /**
     * Signal that you are done using this session. In the default driver usage, closing and accessing sessions is
     * very low cost.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. Stage is completed when all outstanding
     * queries in the session have completed, meaning any writes you performed are guaranteed to be durably stored.
     * It might be completed exceptionally when there are unconsumed errors from previous queries or transactions.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();
}
