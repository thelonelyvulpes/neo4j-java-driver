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
package org.neo4j.driver.internal.async;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;

final class DelegatingAsyncTransactionContext implements AsyncTransactionContext {
    private final AsyncTransaction delegate;

    public DelegatingAsyncTransactionContext(AsyncTransaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query, Value parameters) {
        return delegate.runAsync(query, parameters);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query, Map<String, Object> parameters) {
        return delegate.runAsync(query, parameters);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query, Record parameters) {
        return delegate.runAsync(query, parameters);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String query) {
        return delegate.runAsync(query);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        return delegate.runAsync(query);
    }
}
