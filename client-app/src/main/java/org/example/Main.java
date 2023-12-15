package org.example;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
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
import org.neo4j.driver.SessionConfig;

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
        var appSpan = tracer.spanBuilder("complete work").setSpanKind(SpanKind.CLIENT).startSpan();

        try (var scope = appSpan.makeCurrent()) {
            try (var driver = GraphDatabase.driver("neo4j://127.0.0.1:20455", AuthTokens.none(), cfg)) {
                org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer.forceSSR = false;
                try (var session = driver.session()) {
                    session.executeWrite(tx -> {
                        var cursor = tx.run("CREATE (:Node)");
                        cursor.consume();

                        var r = tx.run("UNWIND range(1, 10) as x return x").stream().toList();
                        return 1;
                    });
                }
            }
            appSpan.end();
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