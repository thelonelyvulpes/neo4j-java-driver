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
package org.neo4j.docs.driver;

import org.neo4j.driver.async.AsyncSession;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class AsyncAutocommitTransactionExample extends BaseApplication {
    public AsyncAutocommitTransactionExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    @SuppressWarnings("CallToPrintStackTrace")
    // tag::async-autocommit-transaction[]
    public CompletionStage<List<String>> readProductTitles() {
        var query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        var parameters = Map.<String, Object>of("id", 0);

        var session = driver.session(AsyncSession.class);

        return session.runAsync(query, parameters)
                .thenCompose(cursor -> cursor.listAsync(record -> record.get(0).asString()))
                .exceptionally(error -> {
                    // query execution failed, print error and fallback to empty list of titles
                    error.printStackTrace();
                    return Collections.emptyList();
                })
                .thenCompose(titles -> session.closeAsync().thenApply(ignore -> titles));
    }
    // end::async-autocommit-transaction[]
}
