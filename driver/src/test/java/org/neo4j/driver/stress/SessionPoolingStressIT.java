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
package org.neo4j.driver.stress;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.GraphDatabase.driver;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class SessionPoolingStressIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private static final int N_THREADS = 50;
    private static final int TEST_TIME = 10000;

    private static final List<String> QUERIES =
            asList("RETURN 1295 + 42", "UNWIND range(1,10000) AS x CREATE (n {prop:x}) DELETE n ");

    private Driver driver;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(N_THREADS, daemon(getClass().getSimpleName() + "-thread-"));
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }

        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void shouldWorkFine() throws Throwable {
        var config = Config.builder().withoutEncryption().build();

        driver = driver(neo4j.uri(), neo4j.authTokenManager(), config);

        var stop = new AtomicBoolean();
        var failureReference = new AtomicReference<Throwable>();

        doWork(stop, failureReference);

        Thread.sleep(TEST_TIME);

        stop.set(true);
        executor.shutdown();
        assertTrue(executor.awaitTermination(90, TimeUnit.SECONDS));

        var failure = failureReference.get();
        if (failure != null) {
            throw new AssertionError("Some workers have failed", failure);
        }
    }

    private void doWork(AtomicBoolean stop, AtomicReference<Throwable> failure) {
        for (var i = 0; i < N_THREADS; i++) {
            executor.execute(new Worker(driver, stop, failure));
        }
    }

    private static class Worker implements Runnable {
        private final Random random = ThreadLocalRandom.current();
        private final Driver driver;
        private final AtomicBoolean stop;
        private final AtomicReference<Throwable> failureReference;

        Worker(Driver driver, AtomicBoolean stop, AtomicReference<Throwable> failureReference) {
            this.driver = driver;
            this.stop = stop;
            this.failureReference = failureReference;
        }

        @Override
        public void run() {
            try {
                while (!stop.get()) {
                    for (var query : QUERIES) {
                        runQuery(query);
                    }
                }
            } catch (Throwable failure) {
                if (!failureReference.compareAndSet(null, failure)) {
                    failureReference.get().addSuppressed(failure);
                }
            }
        }

        private void runQuery(String query) throws InterruptedException {
            try (var session = driver.session()) {
                var run = session.run(query);
                Thread.sleep(random.nextInt(100));
                run.consume();
                Thread.sleep(random.nextInt(100));
            }
        }
    }
}
