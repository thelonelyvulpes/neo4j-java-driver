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
package org.neo4j.driver.internal;

import java.util.Map;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.MapValue;

public abstract class AbstractQueryRunner implements QueryRunner {
    @Override
    public final Result run(String query, Value parameters) {
        return run(new Query(query, parameters));
    }

    @Override
    public final Result run(String query, Map<String, Object> parameters) {
        return run(query, parameters(parameters));
    }

    @Override
    public final Result run(String query, Record parameters) {
        return run(query, parameters(parameters));
    }

    @Override
    public final Result run(String query) {
        return run(query, Values.EmptyMap);
    }

    public static Value parameters(Record record) {
        return record == null ? Values.EmptyMap : parameters(record.asMap());
    }

    public static Value parameters(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return Values.EmptyMap;
        }
        return new MapValue(Extract.mapOfValues(map));
    }
}
