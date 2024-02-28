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
package org.neo4j.driver.testutil;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.stream.Collectors.toSet;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.types.IsoDuration;

public final class TemporalUtil {
    /**
     * A list of zone ids whose use might lead to unexpected results.
     * <p>
     * Some ids were removed from the tz database and neo4j can re-map such ids to other ids.
     * For example "Canada/East-Saskatchewan" will be returned as "Canada/Saskatchewan".
     * Other time zones are not working correctly in some Java versions
     */
    private static final List<String> EXCLUDED_ZONE_IDS = Arrays.asList(
            "Canada/East-Saskatchewan",
            "Chile/EasterIsland",
            "Africa/Casablanca",
            "tzid",
            "Asia/Qostanay",
            "America/Santiago", // Can cause flakyness on windows, see
            // https://stackoverflow.com/questions/37533796/java-calendar-returns-wrong-hour-in-ms-windows-for-america-santiago-zone.
            "US/Pacific-New", // Appeared out of nothing in windows, does not test reliable
            // "org.neo4j.driver.exceptions.ClientException: Unable to construct ZonedDateTime value:
            // `Unknown time-zone ID: US/Pacific-New`"
            "Pacific/Easter",
            "Pacific/Kanton",
            "Asia/Gaza",
            "Antarctica/Macquarie",
            "America/Ciudad_Juarez",
            "Europe/Kyiv" // might have issues when running with dated TZ db
            );

    private TemporalUtil() {}

    public static LocalDate randomLocalDate() {
        return LocalDate.of(random(YEAR), random(MONTH_OF_YEAR), random(DAY_OF_MONTH));
    }

    public static OffsetTime randomOffsetTime() {
        var offset = randomZoneOffset();
        return OffsetTime.of(
                random(HOUR_OF_DAY), random(MINUTE_OF_HOUR), random(SECOND_OF_MINUTE), random(NANO_OF_SECOND), offset);
    }

    public static LocalTime randomLocalTime() {
        return LocalTime.of(
                random(HOUR_OF_DAY), random(MINUTE_OF_HOUR), random(SECOND_OF_MINUTE), random(NANO_OF_SECOND));
    }

    public static LocalDateTime randomLocalDateTime() {
        return LocalDateTime.of(
                random(YEAR),
                random(MONTH_OF_YEAR),
                random(DAY_OF_MONTH),
                random(HOUR_OF_DAY),
                random(MINUTE_OF_HOUR),
                random(SECOND_OF_MINUTE),
                random(NANO_OF_SECOND));
    }

    public static OffsetDateTime randomOffsetDateTime() {
        return randomZonedDateTimeWithOffset().toOffsetDateTime();
    }

    public static ZonedDateTime randomZonedDateTimeWithOffset() {
        return randomZonedDateTime(randomZoneOffset());
    }

    public static ZonedDateTime randomZonedDateTimeWithZoneId() {
        return randomZonedDateTime(randomZoneId());
    }

    public static IsoDuration randomDuration() {
        var sign = random().nextBoolean() ? 1 : -1; // duration can be negative
        return new InternalIsoDuration(
                sign * randomInt(), sign * randomInt(), sign * randomInt(), Math.abs(random(NANO_OF_SECOND)));
    }

    private static ZonedDateTime randomZonedDateTime(ZoneId zoneId) {
        return ZonedDateTime.of(
                random(YEAR),
                random(MONTH_OF_YEAR),
                random(DAY_OF_MONTH),
                random(HOUR_OF_DAY),
                random(MINUTE_OF_HOUR),
                random(SECOND_OF_MINUTE),
                random(NANO_OF_SECOND),
                zoneId);
    }

    private static ZoneOffset randomZoneOffset() {
        var min = ZoneOffset.MIN.getTotalSeconds();
        var max = ZoneOffset.MAX.getTotalSeconds();
        return ZoneOffset.ofTotalSeconds(random().nextInt(min, max));
    }

    private static ZoneId randomZoneId() {
        var availableZoneIds = ZoneId.getAvailableZoneIds().stream()
                .filter(id -> !EXCLUDED_ZONE_IDS.contains(id))
                .collect(toSet());

        var randomIndex = random().nextInt(availableZoneIds.size());
        var index = 0;
        for (var id : availableZoneIds) {
            if (index == randomIndex) {
                return ZoneId.of(id);
            } else {
                index++;
            }
        }
        throw new AssertionError("Unable to pick random ZoneId from the set of available ids: " + availableZoneIds);
    }

    private static int random(ChronoField field) {
        var range = field.range();
        var min = range.getMinimum();
        var max = range.getSmallestMaximum();
        var value = random().nextLong(min, max);
        return Math.toIntExact(value);
    }

    private static int randomInt() {
        return random().nextInt(0, Integer.MAX_VALUE);
    }

    private static ThreadLocalRandom random() {
        return ThreadLocalRandom.current();
    }
}
