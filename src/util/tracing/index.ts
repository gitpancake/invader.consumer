import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from "@opentelemetry/semantic-conventions";

const TEMPO_ENDPOINT = process.env.TEMPO_HTTP_ENDPOINT || "http://tempo:4318/v1/traces";

export function initTracing(): NodeSDK | null {
  if (!process.env.TEMPO_HTTP_ENDPOINT) {
    console.log("[Tracing] TEMPO_HTTP_ENDPOINT not set, tracing disabled");
    return null;
  }

  console.log("[Tracing] Initializing OpenTelemetry, sending traces to " + TEMPO_ENDPOINT);

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: "invaders-consumer",
      [ATTR_SERVICE_VERSION]: "1.0.0",
    }),
    traceExporter: new OTLPTraceExporter({
      url: TEMPO_ENDPOINT,
    }),
    instrumentations: [
      getNodeAutoInstrumentations({
        "@opentelemetry/instrumentation-fs": { enabled: false },
        "@opentelemetry/instrumentation-http": { enabled: true },
        "@opentelemetry/instrumentation-pg": { enabled: true },
        "@opentelemetry/instrumentation-amqplib": { enabled: true },
      }),
    ],
  });

  sdk.start();
  console.log("[Tracing] OpenTelemetry initialized successfully");

  process.on("SIGTERM", () => {
    sdk.shutdown()
      .then(() => console.log("[Tracing] OpenTelemetry shut down"))
      .catch((err) => console.error("[Tracing] Error shutting down", err));
  });

  return sdk;
}
