package org.example;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
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

public class Main {

    public static void main(String[] args) {
        var otb = makeBuilder();

        try (var ot = otb.buildAndRegisterGlobal()) {
            Tracer tracer = ot.getTracer("app", "0.1.0");
            runDriver(ot, tracer);
        }
    }

    private static void runDriver(OpenTelemetry ot, Tracer tracer) {
        var cfg = Config.builder().withOpenTelemetry(ot).build();

        try (var driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none(), cfg)) {
            try (var session = driver.session()) {
                session.executeRead(tx -> {
                    var cursor = tx.run("RETURN 1 as n");
                    cursor.consume();
                    return 1;
                });
            }
        }
    }

    private static OpenTelemetrySdkBuilder makeBuilder() {
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