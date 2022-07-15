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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;

public class SessionFactoryImpl implements SessionFactory {
    private final ConnectionProvider connectionProvider;
    private final RetryLogic retryLogic;
    private final Logging logging;
    private final boolean leakedSessionsLoggingEnabled;
    private final long defaultFetchSize;

    SessionFactoryImpl(ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config) {
        this.connectionProvider = connectionProvider;
        this.leakedSessionsLoggingEnabled = config.logLeakedSessions();
        this.retryLogic = retryLogic;
        this.logging = config.logging();
        this.defaultFetchSize = config.fetchSize();
    }

    @Override
    public NetworkSession newInstance(SessionConfig sessionConfig) {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder(toDistinctSet(sessionConfig.bookmarks()));
        return createSession(
                connectionProvider,
                retryLogic,
                parseDatabaseName(sessionConfig),
                sessionConfig.defaultAccessMode(),
                bookmarksHolder,
                parseFetchSize(sessionConfig),
                sessionConfig.impersonatedUser().orElse(null),
                logging);
    }

    private Set<Bookmark> toDistinctSet(Iterable<Bookmark> bookmarks) {
        Set<Bookmark> set = new HashSet<>();
        if (bookmarks != null) {
            for (Bookmark bookmark : bookmarks) {
                if (bookmark != null) {
                    Set<String> values = bookmark.values();
                    int size = values.size();
                    if (size == 1) {
                        set.add(bookmark);
                    } else if (size > 1) {
                        for (String value : values) {
                            set.add(Bookmark.from(value));
                        }
                    }
                }
            }
        }
        return Collections.unmodifiableSet(set);
    }

    private long parseFetchSize(SessionConfig sessionConfig) {
        return sessionConfig.fetchSize().orElse(defaultFetchSize);
    }

    private DatabaseName parseDatabaseName(SessionConfig sessionConfig) {
        return sessionConfig
                .database()
                .flatMap(name -> Optional.of(DatabaseNameUtil.database(name)))
                .orElse(DatabaseNameUtil.defaultDatabase());
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return connectionProvider.verifyConnectivity();
    }

    @Override
    public CompletionStage<Void> close() {
        return connectionProvider.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return connectionProvider.supportsMultiDb();
    }

    @Override
    public CompletionStage<Boolean> supportsAutomaticQueryRouting() {
        return connectionProvider.supportsAutoQueryRouting();
    }
    /**
     * Get the underlying connection provider.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the connection provider used by this factory.
     */
    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    private NetworkSession createSession(
            ConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            BookmarksHolder bookmarksHolder,
            long fetchSize,
            String impersonatedUser,
            Logging logging) {
        return leakedSessionsLoggingEnabled
                ? new LeakLoggingNetworkSession(
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarksHolder,
                        impersonatedUser,
                        fetchSize,
                        logging)
                : new NetworkSession(
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarksHolder,
                        impersonatedUser,
                        fetchSize,
                        logging);
    }
}
