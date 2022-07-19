package org.neo4j.driver;

import java.time.Duration;

public record RetryDelay(boolean retry, Duration delay) {
}
