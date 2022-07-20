package org.neo4j.driver;

import java.time.Duration;
import java.util.function.Function;

public final class Retries {
    public static final Function<RetryInfo, RetryDelay> transientFunctions = info ->
            info.attempts() > info.maxRetry()
                    ? new RetryDelay(true, Duration.ofMillis(info.attempts() * 100))
                    : new RetryDelay(false, Duration.ZERO);

    public static final Function<RetryInfo, RetryDelay> noRetry = info ->
            new RetryDelay(false, Duration.ZERO);
}
