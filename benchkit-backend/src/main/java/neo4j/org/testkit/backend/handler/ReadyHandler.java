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
package neo4j.org.testkit.backend.handler;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

public class ReadyHandler {
    private final Driver driver;
    private final Logger logger;

    public ReadyHandler(Driver driver, Logging logging) {
        this.driver = driver;
        this.logger = logging.getLog(getClass());
    }

    public CompletionStage<FullHttpResponse> ready(HttpVersion httpVersion) {
        return CompletableFuture.completedStage(null)
                .thenComposeAsync(ignored -> driver.verifyConnectivityAsync())
                .handle((ignored, throwable) -> {
                    HttpResponseStatus status;
                    if (throwable != null) {
                        logger.error("An error occured during workload handling.", throwable);
                        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                    } else {
                        status = HttpResponseStatus.NO_CONTENT;
                    }
                    return new DefaultFullHttpResponse(httpVersion, status);
                });
    }
}
