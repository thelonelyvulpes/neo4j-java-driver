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
import org.neo4j.driver.summary.ResultSummary;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class AsyncTransactionFunctionExample extends BaseApplication {
    public AsyncTransactionFunctionExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::async-transaction-function[]
    public CompletionStage<ResultSummary> printAllProducts() {
        var query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String, Object> parameters = Collections.singletonMap("id", 0);

        var session = driver.session(AsyncSession.class);

        return session.executeReadAsync(tx -> tx.runAsync(query, parameters)
                .thenCompose(cursor -> cursor.forEachAsync(record ->
                        // asynchronously print every record
                        System.out.println(record.get(0).asString()))));
    }
    // end::async-transaction-function[]
}
