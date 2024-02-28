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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.forDatabase;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Session;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class SummaryIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Session session;

    @BeforeEach
    @SuppressWarnings("resource")
    void setup() {
        session = neo4j.driver().session();
    }

    @AfterEach
    void tearDown() {
        if (session != null && session.isOpen()) {
            session.close();
        }
        session = null;
    }

    @Test
    void shouldContainBasicMetadata() {
        // Given
        var parameters = Values.parameters("limit", 10);
        var query = "UNWIND [1, 2, 3, 4] AS n RETURN n AS number LIMIT $limit";

        // When
        var result = session.run(query, parameters);

        // Then
        assertTrue(result.hasNext());

        // When
        var summary = result.consume();

        // Then
        assertThat(summary.queryType(), equalTo(QueryType.READ_ONLY));
        assertThat(summary.query().text(), equalTo(query));
        assertThat(summary.query().parameters(), equalTo(parameters));
        assertFalse(summary.hasPlan());
        assertFalse(summary.hasProfile());
        assertThat(summary, equalTo(result.consume()));
    }

    @Test
    void shouldContainTimeInformation() {
        // Given
        var summary =
                session.run("UNWIND range(1,1000) AS n RETURN n AS number").consume();

        // Then
        assertThat(summary.resultAvailableAfter(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(0L));
        assertThat(summary.resultConsumedAfter(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(0L));
    }

    @Test
    void shouldContainCorrectStatistics() {
        assertThat(session.run("CREATE (n)").consume().counters().nodesCreated(), equalTo(1));
        assertThat(session.run("MATCH (n) DELETE (n)").consume().counters().nodesDeleted(), equalTo(1));

        assertThat(session.run("CREATE ()-[:KNOWS]->()").consume().counters().relationshipsCreated(), equalTo(1));
        assertThat(
                session.run("MATCH ()-[r:KNOWS]->() DELETE r")
                        .consume()
                        .counters()
                        .relationshipsDeleted(),
                equalTo(1));

        assertThat(session.run("CREATE (n:ALabel)").consume().counters().labelsAdded(), equalTo(1));
        assertThat(session.run("CREATE (n {magic: 42})").consume().counters().propertiesSet(), equalTo(1));
        assertTrue(session.run("CREATE (n {magic: 42})").consume().counters().containsUpdates());
        assertThat(
                session.run("MATCH (n:ALabel) REMOVE n:ALabel ")
                        .consume()
                        .counters()
                        .labelsRemoved(),
                equalTo(1));

        if (neo4j.isNeo4j43OrEarlier()) {
            assertThat(
                    session.run("CREATE INDEX ON :ALabel(prop)")
                            .consume()
                            .counters()
                            .indexesAdded(),
                    equalTo(1));
            assertThat(
                    session.run("DROP INDEX ON :ALabel(prop)")
                            .consume()
                            .counters()
                            .indexesRemoved(),
                    equalTo(1));

            assertThat(
                    session.run("CREATE CONSTRAINT restrictedConstraint ON (book:Book) ASSERT book.isbn IS UNIQUE")
                            .consume()
                            .counters()
                            .constraintsAdded(),
                    equalTo(1));
            assertThat(
                    session.run("DROP CONSTRAINT restrictedConstraint")
                            .consume()
                            .counters()
                            .constraintsRemoved(),
                    equalTo(1));
        } else {
            assertThat(
                    session.run("CREATE INDEX superIndex FOR (n:ALabel) ON (n.prop)")
                            .consume()
                            .counters()
                            .indexesAdded(),
                    equalTo(1));
            assertThat(session.run("DROP INDEX superIndex").consume().counters().indexesRemoved(), equalTo(1));

            assertThat(
                    session.run("CREATE CONSTRAINT restrictedConstraint FOR (book:Book) REQUIRE book.isbn IS UNIQUE")
                            .consume()
                            .counters()
                            .constraintsAdded(),
                    equalTo(1));
            assertThat(
                    session.run("DROP CONSTRAINT restrictedConstraint")
                            .consume()
                            .counters()
                            .constraintsRemoved(),
                    equalTo(1));
        }
    }

    @Test
    @EnabledOnNeo4jWith(Neo4jFeature.BOLT_V4)
    @SuppressWarnings("resource")
    void shouldGetSystemUpdates() {
        try (var session = neo4j.driver().session(forDatabase("system"))) {
            var result = session.run("CREATE USER foo SET PASSWORD 'Testing0'");
            assertThat(result.consume().counters().containsUpdates(), equalTo(false));
            assertThat(result.consume().counters().containsSystemUpdates(), equalTo(true));
        }
    }

    @Test
    void shouldContainCorrectQueryType() {
        assertThat(session.run("MATCH (n) RETURN 1").consume().queryType(), equalTo(QueryType.READ_ONLY));
        assertThat(session.run("CREATE (n)").consume().queryType(), equalTo(QueryType.WRITE_ONLY));
        assertThat(session.run("CREATE (n) RETURN (n)").consume().queryType(), equalTo(QueryType.READ_WRITE));
        if (neo4j.isNeo4j43OrEarlier()) {
            assertThat(session.run("CREATE INDEX ON :User(p)").consume().queryType(), equalTo(QueryType.SCHEMA_WRITE));
        } else {
            assertThat(
                    session.run("CREATE INDEX FOR (n:User) ON n.p").consume().queryType(),
                    equalTo(QueryType.SCHEMA_WRITE));
        }
    }

    @Test
    void shouldContainCorrectPlan() {
        // When
        var summary = session.run("EXPLAIN MATCH (n) RETURN 1").consume();

        // Then
        assertTrue(summary.hasPlan());

        var plan = summary.plan();
        assertThat(plan.operatorType(), notNullValue());
        assertThat(plan.identifiers().size(), greaterThan(0));
        assertThat(plan.arguments().size(), greaterThan(0));
        assertThat(plan.children().size(), greaterThan(0));
    }

    @Test
    void shouldContainProfile() {
        // When
        var summary = session.run("PROFILE RETURN 1").consume();

        // Then
        assertTrue(summary.hasProfile());
        assertTrue(
                summary.hasPlan()); // Profile is a superset of plan, so plan should be available as well if profile is
        // available
        assertEquals(summary.plan(), summary.profile());

        var profile = summary.profile();

        assertEquals(0, profile.time());
        assertEquals(0, profile.dbHits());
        assertEquals(1, profile.records());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldContainNotifications() {
        // When
        var summary =
                session.run("EXPLAIN MATCH (n:ThisLabelDoesNotExist) RETURN n").consume();

        // Then
        var notifications = summary.notifications();
        assertNotNull(notifications);
        assertThat(notifications.size(), equalTo(1));
        var notification = notifications.get(0);
        assertThat(notification.code(), notNullValue());
        assertThat(notification.title(), notNullValue());
        assertThat(notification.description(), notNullValue());
        assertThat(notification.severity(), notNullValue());
        assertThat(notification.severityLevel(), notNullValue());
        assertThat(notification.rawSeverityLevel(), notNullValue());
        assertThat(notification.position(), notNullValue());
    }

    @Test
    void shouldContainNoNotifications() {
        // When
        var summary = session.run("RETURN 1").consume();

        // Then
        assertThat(summary.notifications().size(), equalTo(0));
    }
}
