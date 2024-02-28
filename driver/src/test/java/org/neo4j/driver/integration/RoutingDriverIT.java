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
package org.neo4j.driver.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.driver.internal.util.Matchers.clusterDriver;

import java.net.URI;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
@EnabledOnNeo4jWith(Neo4jFeature.BOLT_V4)
class RoutingDriverIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldBeAbleToConnectSingleInstanceWithNeo4jScheme() {
        var uri = URI.create(String.format(
                "neo4j://%s:%s", neo4j.uri().getHost(), neo4j.uri().getPort()));

        try (var driver = GraphDatabase.driver(uri, neo4j.authTokenManager());
                var session = driver.session()) {
            assertThat(driver, is(clusterDriver()));

            var result = session.run("RETURN 1");
            assertThat(result.single().get(0).asInt(), CoreMatchers.equalTo(1));
        }
    }

    @Test
    void shouldBeAbleToRunQueryOnNeo4j() {
        var uri = URI.create(String.format(
                "neo4j://%s:%s", neo4j.uri().getHost(), neo4j.uri().getPort()));
        try (var driver = GraphDatabase.driver(uri, neo4j.authTokenManager());
                var session = driver.session(forDatabase("neo4j"))) {
            assertThat(driver, is(clusterDriver()));

            var result = session.run("RETURN 1");
            assertThat(result.single().get(0).asInt(), CoreMatchers.equalTo(1));
        }
    }
}
