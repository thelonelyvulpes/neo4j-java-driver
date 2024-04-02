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
package org.example;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;

public class Main {

    public static void main(String[] args) {
        var address = "neo4j://127.0.0.1:20017";
        var badRoute = false;

        var otb = createOpenTelemetry();
        try (var ot = otb.buildAndRegisterGlobal()) {
            runDriver(ot, address, badRoute);
        }
    }

    public static void runDriver(OpenTelemetry ot, String address, boolean badRoute) {
        var cfg = Config.builder().withOpenTelemetry(ot).build();
        try (var driver = GraphDatabase.driver(address, AuthTokens.none(), cfg)) {
            org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer.forceSSR = badRoute;
            try (var session = driver.session(SessionConfig.builder().withDatabase("neo4j").build())) {
                session.executeWrite(tx -> {
                    var cursor = tx.run("CREATE (:Node)");
                    cursor.consume();
                    var ignored = tx.run("UNWIND range(1, 10) as x return x").stream().toList();
                    return 1;
                });
            }

            try (var session = driver.session(SessionConfig.builder().withDatabase("neo4j").build())) {
                session.executeWrite(tx -> {
                    var cursor = tx.run("CREATE (:Node)");
                    cursor.consume();
                    var ignored = tx.run("UNWIND range(1, 10) as x return x").stream().toList();
                    return 1;
                });
            }
        }
    }


    private static OpenTelemetrySdkBuilder createOpenTelemetry() {
        Resource resource = Resource.getDefault()
                .toBuilder()
                .put(ResourceAttributes.SERVICE_NAME, "client-app")
                .build();

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(
                                OtlpGrpcSpanExporter.builder()
                                        .setEndpoint("http://localhost:4317")
                                        .build())
                        .build())
                .setResource(resource)
                .build();
        return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider);
    }
}

