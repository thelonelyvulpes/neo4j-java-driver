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

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.NotificationCategory;

public record InternalNotificationCategory(Type type) implements NotificationCategory {

    public InternalNotificationCategory {
        Objects.requireNonNull(type, "type must not be null");
    }

    public enum Type {
        HINT,
        UNRECOGNIZED,
        UNSUPPORTED,
        PERFORMANCE,
        DEPRECATION,
        SECURITY,
        TOPOLOGY,
        GENERIC
    }

    public static Optional<NotificationCategory> valueOf(String value) {
        return Arrays.stream(Type.values())
                .filter(type -> type.toString().equals(value))
                .findFirst()
                .map(type -> switch (type) {
                    case HINT -> NotificationCategory.HINT;
                    case UNRECOGNIZED -> NotificationCategory.UNRECOGNIZED;
                    case UNSUPPORTED -> NotificationCategory.UNSUPPORTED;
                    case PERFORMANCE -> NotificationCategory.PERFORMANCE;
                    case DEPRECATION -> NotificationCategory.DEPRECATION;
                    case SECURITY -> NotificationCategory.SECURITY;
                    case TOPOLOGY -> NotificationCategory.TOPOLOGY;
                    case GENERIC -> NotificationCategory.GENERIC;
                });
    }
}
