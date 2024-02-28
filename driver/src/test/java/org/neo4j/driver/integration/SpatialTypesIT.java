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

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.ofPoint;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.internal.util.Neo4jFeature.SPATIAL_TYPES;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;
import org.neo4j.driver.types.Point;

@EnabledOnNeo4jWith(SPATIAL_TYPES)
@ParallelizableIT
class SpatialTypesIT {
    private static final int WGS_84_CRS_CODE = 4326;
    private static final int CARTESIAN_CRS_CODE = 7203;
    private static final double DELTA = 0.00001;

    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldReceivePoint() {
        var record = session.run("RETURN point({x: 39.111748, y:-76.775635})").single();

        var point = record.get(0).asPoint();

        assertEquals(CARTESIAN_CRS_CODE, point.srid());
        assertEquals(39.111748, point.x(), DELTA);
        assertEquals(-76.775635, point.y(), DELTA);
    }

    @Test
    void shouldSendPoint() {
        var pointValue = point(WGS_84_CRS_CODE, 38.8719, 77.0563);
        var record1 = session.run("CREATE (n:Node {location: $point}) RETURN 42", singletonMap("point", pointValue))
                .single();

        assertEquals(42, record1.get(0).asInt());

        var record2 = session.run("MATCH (n:Node) RETURN n.location").single();
        var point = record2.get(0).asPoint();

        assertEquals(WGS_84_CRS_CODE, point.srid());
        assertEquals(38.8719, point.x(), DELTA);
        assertEquals(77.0563, point.y(), DELTA);
    }

    @Test
    void shouldSendAndReceivePoint() {
        testPointSendAndReceive(point(CARTESIAN_CRS_CODE, 40.7624, 73.9738));
    }

    @Test
    void shouldSendAndReceiveRandom2DPoints() {
        var randomPoints = ThreadLocalRandom.current().ints(1_000, 0, 2).mapToObj(SpatialTypesIT::createPoint);

        randomPoints.forEach(this::testPointSendAndReceive);
    }

    @Test
    void shouldSendAndReceiveRandom2DPointArrays() {
        var randomPointLists = ThreadLocalRandom.current().ints(1_000, 0, 2).mapToObj(SpatialTypesIT::randomPointList);

        randomPointLists.forEach(this::testPointListSendAndReceive);
    }

    private void testPointSendAndReceive(Value pointValue) {
        var originalPoint = pointValue.asPoint();

        var record = session.run("CREATE (n {point: $point}) return n.point", singletonMap("point", pointValue))
                .single();
        var receivedPoint = record.get(0).asPoint();

        assertPoints2DEqual(originalPoint, receivedPoint);
    }

    private void testPointListSendAndReceive(List<Value> points) {
        var record = session.run("CREATE (n {points: $points}) return n.points", singletonMap("points", points))
                .single();
        var receivedPoints = record.get(0).asList(ofPoint());

        assertEquals(points.size(), receivedPoints.size());
        for (var i = 0; i < points.size(); i++) {
            assertPoints2DEqual(points.get(i).asPoint(), receivedPoints.get(i));
        }
    }

    private static List<Value> randomPointList(int index) {
        var size = ThreadLocalRandom.current().nextInt(1, 100);
        return IntStream.range(0, size).mapToObj(ignored -> createPoint(index)).collect(toList());
    }

    private static Value createPoint(int idx) {
        return idx % 2 == 0
                ? point(CARTESIAN_CRS_CODE, randomDouble(), randomDouble())
                : point(WGS_84_CRS_CODE, randomDouble(), randomDoubleWGS_84_Y());
    }

    private static double randomDouble() {
        return ThreadLocalRandom.current().nextDouble(-180.0, 180);
    }

    private static double randomDoubleWGS_84_Y() {
        return ThreadLocalRandom.current().nextDouble(-90.0, 90);
    }

    private static void assertPoints2DEqual(Point expected, Point actual) {
        var message = "Expected: " + expected + " but was: " + actual;
        assertEquals(expected.srid(), actual.srid(), message);
        assertEquals(expected.x(), actual.x(), DELTA, message);
        assertEquals(expected.y(), actual.y(), DELTA, message);
    }
}
