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
package org.neo4j.driver.internal.util;

import io.netty.util.concurrent.EventExecutorGroup;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.retry.RetryLogic;

public class DriverFactoryWithFixedRetryLogic extends DriverFactory {
    private final int retryCount;

    public DriverFactoryWithFixedRetryLogic(int retryCount) {
        this.retryCount = retryCount;
    }

    @Override
    protected RetryLogic createRetryLogic(
            long maxTransactionRetryTime, EventExecutorGroup eventExecutorGroup, Logging logging) {
        return new FixedRetryLogic(retryCount);
    }
}
