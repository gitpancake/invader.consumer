import http from "http";
import {
    Registry,
    Counter,
    Gauge,
    Histogram,
    collectDefaultMetrics,
} from "prom-client";

// Create a new registry
export const register = new Registry();

// Collect default Node.js metrics (CPU, memory, event loop, etc.)
collectDefaultMetrics({ register });

// ============================================
// COUNTERS
// ============================================

export const flashesProcessedTotal = new Counter({
    name: "invaders_consumer_flashes_processed_total",
    help: "Total flashes processed",
    registers: [register],
});

export const flashesFailedTotal = new Counter({
    name: "invaders_consumer_flashes_failed_total",
    help: "Failed flash processing",
    registers: [register],
});

export const ipfsUploadsTotal = new Counter({
    name: "invaders_consumer_ipfs_uploads_total",
    help: "Successful IPFS uploads",
    registers: [register],
});

export const ipfsFailuresTotal = new Counter({
    name: "invaders_consumer_ipfs_failures_total",
    help: "Failed IPFS uploads",
    registers: [register],
});

// ============================================
// GAUGES
// ============================================

export const queueDepth = new Gauge({
    name: "invaders_consumer_queue_depth",
    help: "Current RabbitMQ queue depth",
    registers: [register],
});

export const processingRate = new Gauge({
    name: "invaders_consumer_processing_rate",
    help: "Flashes processed per second (rolling average)",
    registers: [register],
});

export const circuitBreakerOpen = new Gauge({
    name: "invaders_consumer_circuit_breaker_open",
    help: "1 if circuit breaker is open, 0 otherwise",
    registers: [register],
});

export const consecutiveFailures = new Gauge({
    name: "invaders_consumer_consecutive_failures",
    help: "Current consecutive failure count",
    labelNames: ["type"],
    registers: [register],
});

export const lastProcessedTimestamp = new Gauge({
    name: "invaders_consumer_last_processed_timestamp",
    help: "Unix timestamp of last processed flash",
    registers: [register],
});

export const uptimeSeconds = new Gauge({
    name: "invaders_consumer_uptime_seconds",
    help: "Process uptime in seconds",
    registers: [register],
});

export const memoryBytes = new Gauge({
    name: "invaders_consumer_memory_bytes",
    help: "Memory usage in bytes",
    labelNames: ["type"],
    registers: [register],
});

// ============================================
// HISTOGRAMS
// ============================================

export const processingDurationSeconds = new Histogram({
    name: "invaders_consumer_processing_duration_seconds",
    help: "Time to process a flash in seconds",
    buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60],
    registers: [register],
});

// ============================================
// METRICS SERVER
// ============================================

const startTime = Date.now();

// Update uptime and memory gauges periodically
setInterval(() => {
    uptimeSeconds.set((Date.now() - startTime) / 1000);

    const mem = process.memoryUsage();
    memoryBytes.set({ type: "heap_used" }, mem.heapUsed);
    memoryBytes.set({ type: "heap_total" }, mem.heapTotal);
    memoryBytes.set({ type: "rss" }, mem.rss);
    memoryBytes.set({ type: "external" }, mem.external);
}, 5000);

export function startMetricsServer(port: number = 9091): void {
    const server = http.createServer(async (req, res) => {
        const url = req.url?.split("?")[0] || "";

        if (url === "/metrics" || url === "/") {
            try {
                res.setHeader("Content-Type", register.contentType);
                res.end(await register.metrics());
            } catch (err) {
                res.statusCode = 500;
                res.end("Error collecting metrics");
            }
        } else if (url === "/health") {
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ status: "ok" }));
        } else {
            res.statusCode = 404;
            res.end("Not found");
        }
    });

    // Bind to localhost only for security - external access via Caddy reverse proxy
    server.listen(port, "127.0.0.1", () => {
        // Logged in main startup banner instead
    });
}
