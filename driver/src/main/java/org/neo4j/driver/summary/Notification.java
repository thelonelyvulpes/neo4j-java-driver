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
package org.neo4j.driver.summary;

import java.util.Optional;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.util.Immutable;

/**
 * Representation for notifications found when executing a query.
 * <p>
 * A notification can be visualized in a client pinpointing problems or other information about the query.
 * @since 1.0
 */
@Immutable
public interface Notification {
    /**
     * Returns a notification code for the discovered issue.
     * @return the notification code
     */
    String code();

    /**
     * Returns a short summary of the notification.
     * @return the title of the notification.
     */
    String title();

    /**
     * Returns a longer description of the notification.
     * @return the description of the notification.
     */
    String description();

    /**
     * The position in the query where this notification points to.
     * Not all notifications have a unique position to point to and in that case the position would be set to null.
     *
     * @return the position in the query where the issue was found, or null if no position is associated with this
     * notification.
     */
    InputPosition position();

    /**
     * The severity level of the notification.
     *
     * @deprecated superseded by {@link #severityLevel()} and {@link #rawSeverityLevel()}
     * @return the severity level of the notification
     */
    @Deprecated
    default String severity() {
        return rawSeverityLevel().orElse("N/A");
    }

    /**
     * Returns the severity level of the notification.
     *
     * @return the severity level of the notification
     * @since 5.7
     */
    default Optional<NotificationSeverity> severityLevel() {
        return Optional.empty();
    }

    /**
     * Returns the raw severity level of the notification as a String returned by the server.
     *
     * @return the severity level of the notification
     * @since 5.7
     */
    default Optional<String> rawSeverityLevel() {
        return Optional.empty();
    }

    /**
     * Returns the category of the notification.
     *
     * @return the category of the notification
     * @since 5.7
     */
    default Optional<NotificationCategory> category() {
        return Optional.empty();
    }

    /**
     * Returns the raw category of the notification as a String returned by the server.
     *
     * @return the category of the notification
     * @since 5.7
     */
    default Optional<String> rawCategory() {
        return Optional.empty();
    }
}
