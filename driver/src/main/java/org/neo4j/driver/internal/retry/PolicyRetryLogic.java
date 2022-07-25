package org.neo4j.driver.internal.retry;

import io.netty.util.concurrent.EventExecutorGroup;
import org.neo4j.driver.RetryDelay;
import org.neo4j.driver.RetryInfo;
import org.neo4j.driver.internal.util.Clock;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class PolicyRetryLogic implements RetryLogic {
    private Clock clock;
    private EventExecutorGroup eventExecutorGroup;
    private Function<RetryInfo, RetryDelay> policy;
    private Integer maxRetries;

    public PolicyRetryLogic(Clock clock,
                            EventExecutorGroup eventExecutorGroup,
                            Function<RetryInfo, RetryDelay> policy, int maxRetries) {
        this.clock = clock;
        this.eventExecutorGroup = eventExecutorGroup;
        this.policy = policy;
        this.maxRetries = maxRetries;
    }

    @Override
    public EventExecutorGroup getExecutorGroup() {
        return this.eventExecutorGroup;
    }

    @Override
    public <T> T retry(Supplier<T> work) {
        List<Throwable> errors = new ArrayList<>();
        int attempt = 0;
        while (true) {
            try {
                return work.get();
            } catch (Exception ex) {
                var delayInfo = policy.apply(new RetryInfo(ex, ++attempt, maxRetries));

                if (!delayInfo.retry()) {
                    if (errors.size() > 1) {
                        addSuppressed(ex, errors);
                    }

                    throw ex;
                }
                errors.add(ex);
                sleep(delayInfo.delay());
            }
        }
    }

    @Override
    public <T> CompletionStage<T> retryAsync(Supplier<CompletionStage<T>> work) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        eventExecutorGroup.next().execute(() -> executeWork(resultFuture, work, 1, new ArrayList<>()));
        return resultFuture;
    }

    private <T> void executeWork(
            CompletableFuture<T> resultFuture,
            Supplier<CompletionStage<T>> work,
            int attempt,
            List<Throwable> errors) {
        CompletionStage<T> workStage;
        try {
            workStage = work.get();
        } catch (Exception error) {
            var retryInfo = policy.apply(new RetryInfo(error, attempt, maxRetries));
            if (retryInfo.retry()) {
                errors.add(error);
                eventExecutorGroup.next().schedule(() ->
                                executeWork(resultFuture, work, attempt + 1, errors),
                        retryInfo.delay().toMillis(), TimeUnit.MILLISECONDS);
                return;
            }

            addSuppressed(error, errors);
            resultFuture.completeExceptionally(error);
            return;
        }

        workStage.whenComplete((result, completionError) -> {
            if (completionError == null) {
                resultFuture.complete(result);
                return;
            }
            if (completionError instanceof Exception ex) {
                var retryInfo = policy.apply(new RetryInfo(ex, attempt, maxRetries));
                if (retryInfo.retry()) {
                    errors.add(ex);
                    eventExecutorGroup
                            .next()
                            .schedule(
                                    () -> executeWork(resultFuture, work, attempt + 1, errors),
                                    retryInfo.delay().toMillis(),
                                    TimeUnit.MILLISECONDS);
                    return;
                }
                addSuppressed(ex, errors);
                resultFuture.completeExceptionally(ex);
                return;
            }
            addSuppressed(completionError, errors);
            resultFuture.completeExceptionally(completionError);
        });
    }

    @Override
    public <T> Publisher<T> retryRx(Publisher<T> work) {
        return Flux.error(new Exception("Not Implemented"));
    }

    private static void addSuppressed(Throwable error, List<Throwable> suppressedErrors) {
        if (suppressedErrors != null) {
            for (Throwable suppressedError : suppressedErrors) {
                if (error != suppressedError) {
                    error.addSuppressed(suppressedError);
                }
            }
        }
    }

    private void sleep(Duration delay) {
        try {
            clock.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Retries interrupted", e);
        }
    }
}
