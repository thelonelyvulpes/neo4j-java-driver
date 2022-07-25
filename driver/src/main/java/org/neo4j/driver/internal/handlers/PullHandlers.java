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
package org.neo4j.driver.internal.handlers;

import org.neo4j.driver.Query;
import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.handlers.pulln.AutoPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;

public class PullHandlers {

    public static PullAllResponseHandler newBoltV3PullAllHandler(
            Query query,
            RunResponseHandler runHandler,
            Connection connection,
            BookmarksHolder bookmarksHolder,
            UnmanagedTransaction tx) {
        return newBoltV3PullAllHandler(query, runHandler, connection, bookmarksHolder, tx, -1);
    }

    public static PullAllResponseHandler newBoltV3PullAllHandler(
            Query query,
            RunResponseHandler runHandler,
            Connection connection,
            BookmarksHolder bookmarksHolder,
            UnmanagedTransaction tx,
            long maxRecordCount) {
        PullResponseCompletionListener completionListener =
                createPullResponseCompletionListener(connection, bookmarksHolder, tx);

        return new LegacyPullAllResponseHandler(
                query, runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, completionListener, maxRecordCount);
    }

    public static PullAllResponseHandler newBoltV4AutoPullHandler(
            Query query,
            RunResponseHandler runHandler,
            Connection connection,
            BookmarksHolder bookmarksHolder,
            UnmanagedTransaction tx,
            long fetchSize) {
        return newBoltV4AutoPullHandler(query, runHandler, connection, bookmarksHolder, tx, fetchSize, -1);
    }

    public static PullAllResponseHandler newBoltV4AutoPullHandler(
            Query query,
            RunResponseHandler runHandler,
            Connection connection,
            BookmarksHolder bookmarksHolder,
            UnmanagedTransaction tx,
            long fetchSize,
            long maxRecordCount) {
        PullResponseCompletionListener completionListener =
                createPullResponseCompletionListener(connection, bookmarksHolder, tx);

        return new AutoPullResponseHandler(
                query, runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, completionListener, fetchSize, maxRecordCount);
    }

    public static PullResponseHandler newBoltV4BasicPullHandler(
            Query query,
            RunResponseHandler runHandler,
            Connection connection,
            BookmarksHolder bookmarksHolder,
            UnmanagedTransaction tx) {
        PullResponseCompletionListener completionListener =
                createPullResponseCompletionListener(connection, bookmarksHolder, tx);

        return new BasicPullResponseHandler(
                query, runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR, completionListener);
    }

    private static PullResponseCompletionListener createPullResponseCompletionListener(
            Connection connection, BookmarksHolder bookmarksHolder, UnmanagedTransaction tx) {
        return tx != null
                ? new TransactionPullResponseCompletionListener(tx)
                : new SessionPullResponseCompletionListener(connection, bookmarksHolder);
    }
}
