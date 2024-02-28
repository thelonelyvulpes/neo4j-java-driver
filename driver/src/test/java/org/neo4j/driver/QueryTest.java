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
package org.neo4j.driver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.parameters;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryTest {
    @Test
    void shouldConstructQueryWithParameters() {
        // given
        var text = "MATCH (n) RETURN n";

        // when
        var query = new Query(text, Values.EmptyMap);

        // then
        assertThat(query.text(), equalTo(text));
        assertThat(query.parameters(), equalTo(Values.EmptyMap));
    }

    @Test
    void shouldConstructQueryWithNoParameters() {
        // given
        var text = "MATCH (n) RETURN n";

        // when
        var query = new Query(text);

        // then
        assertThat(query.text(), equalTo(text));
        assertThat(query.parameters(), equalTo(Values.EmptyMap));
    }

    @Test
    void shouldUpdateQueryText() {
        // when
        var query = new Query("MATCH (n) RETURN n").withText("BOO");

        // then
        assertThat(query.text(), equalTo("BOO"));
        assertThat(query.parameters(), equalTo(Values.EmptyMap));
    }

    @Test
    void shouldReplaceQueryParameters() {
        // when
        var text = "MATCH (n) RETURN n";
        var initialParameters = parameters("a", 1, "b", 2);
        var query = new Query("MATCH (n) RETURN n").withParameters(initialParameters);

        // then
        assertThat(query.text(), equalTo(text));
        assertThat(query.parameters(), equalTo(initialParameters));
    }

    @Test
    void shouldReplaceMapParameters() {
        // when
        var text = "MATCH (n) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("a", 1);
        var query = new Query("MATCH (n) RETURN n").withParameters(parameters);

        // then
        assertThat(query.text(), equalTo(text));
        assertThat(query.parameters(), equalTo(Values.value(parameters)));
    }

    @Test
    void shouldUpdateQueryParameters() {
        // when
        var text = "MATCH (n) RETURN n";
        var initialParameters = parameters("a", 1, "b", 2, "c", 3);
        var query = new Query("MATCH (n) RETURN n", initialParameters)
                .withUpdatedParameters(parameters("a", 0, "b", Values.NULL));

        // then
        assertThat(query.text(), equalTo(text));
        assertThat(query.parameters(), equalTo(parameters("a", 0, "c", 3)));
    }

    @Test
    void shouldProhibitNullQuery() {
        assertThrows(IllegalArgumentException.class, () -> new Query(null));
    }

    @Test
    void shouldProhibitEmptyQuery() {
        assertThrows(IllegalArgumentException.class, () -> new Query(""));
    }
}
