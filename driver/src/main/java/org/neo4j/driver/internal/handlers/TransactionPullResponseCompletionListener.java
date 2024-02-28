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
package org.neo4j.driver.internal.handlers;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.async.UnmanagedTransaction;

public class TransactionPullResponseCompletionListener implements PullResponseCompletionListener {
    private final UnmanagedTransaction tx;

    public TransactionPullResponseCompletionListener(UnmanagedTransaction tx) {
        this.tx = requireNonNull(tx);
    }

    @Override
    public void afterSuccess(Map<String, Value> metadata) {}

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    public void afterFailure(Throwable error) {
        // always mark transaction as terminated because every error is "acknowledged" with a RESET message
        // so database forgets about the transaction after the first error
        // such transaction should not attempt to commit and can be considered as rolled back
        tx.markTerminated(error);
    }
}
