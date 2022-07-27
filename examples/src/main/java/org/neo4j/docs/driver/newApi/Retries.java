package org.neo4j.docs.driver.newApi;

import org.neo4j.driver.Driver;
import org.neo4j.driver.DriverQueryConfig;
import org.neo4j.driver.RetryDelay;
import org.neo4j.driver.RetryInfo;

import java.time.Duration;
import java.util.function.Function;

public class Retries {
    Driver driver;
    private static Thread resetThread;

    public Retries(Driver driver) {
        this.driver = driver;
        beginReset();
    }

    private void beginReset() {
        if (resetThread == null) {
            resetThread = new Thread(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                count = 0;
            });
            resetThread.start();
        }
    }

    static long count = 0;

    static boolean onRetry() {
        return ++count > 10;
    }

    static void stop() {
        resetThread.interrupt();
    }

    public void retry() {
        var retry = new Function<RetryInfo, RetryDelay>() {
            @Override
            public RetryDelay apply(RetryInfo retryInfo) {
                var tooManyRetries = onRetry();
                if (tooManyRetries || retryInfo.attempts() < retryInfo.maxRetry()) {
                    return new RetryDelay(false, Duration.ZERO);
                }

                return new RetryDelay(true, Duration.ofMillis(retryInfo.attempts() * 100));
            }
        };

        var retryConfig = DriverQueryConfig.builder().withRetryFunction(retry).build();
        var res = driver.query("MATCH (n) RETURN n", retryConfig);

        stop();
    }
}
