import { config } from "dotenv";

// Load environment variables FIRST before any other imports
config({ path: ".env" });

import { ConsumeMessage } from "amqplib";
import axios from "axios";
import { Flash } from "./types/Flash";
import { BatchUpdater, BatchUpdateResult } from "./util/batch-updater";
import { databasePool } from "./util/database/connection-pool";
import { proxyRotator } from "./util/proxy";
import { RabbitMQBaseConsumer } from "./util/rabbitmq";
import { RateLimiter } from "./util/rate-limiter";
import { memoryOptimizer } from "./util/memory/optimizer";
import {
    ConcurrentProcessor,
    createImageProcessor,
} from "./util/concurrent/processor";
import { metricsCollector } from "./util/monitoring/metrics";
import {
    createHealthChecker,
    HealthChecker,
} from "./util/monitoring/health-checker";
import { configManager, validateEnvironment } from "./util/config";
import { circuitBreakerRegistry } from "./util/circuit-breaker";
import { startHealthApi } from "./util/health-api";

// Validate environment before starting
validateEnvironment();

const PINATA_JWT = process.env.PINATA_JWT;
if (!PINATA_JWT) {
    throw new Error("PINATA_JWT environment variable is required");
}

const BASE_URL = "https://api.space-invaders.com";

// Enhanced user agents with more variety
const USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
];

// Optimized referrers
const REFERRERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://www.reddit.com/",
    "https://www.github.com/",
    "https://www.stackoverflow.com/",
];

class EnhancedFlashConsumer extends RabbitMQBaseConsumer {
    private batchUpdater: BatchUpdater;
    private consecutiveApiFailures: number = 0;
    private healthChecker: HealthChecker;
    private processingRateLimiter: RateLimiter;
    private concurrentProcessor: ConcurrentProcessor<Flash, void>;
    private batchResultCheckInterval: NodeJS.Timeout | null = null;
    private shutdownRequested: boolean = false;
    private activeProcessing: number = 0;

    constructor() {
        super();

        // Get configuration
        const config = configManager.get();

        console.log(
            "[EnhancedFlashConsumer] Initializing with configuration:",
            configManager.getSummary(),
        );

        // Initialize components
        this.batchUpdater = new BatchUpdater(
            databasePool.getPool(),
            config.processing.batchSize,
        );
        this.healthChecker = createHealthChecker(
            databasePool.getPool(),
            config.monitoring.interval,
        );
        this.processingRateLimiter = new RateLimiter(
            config.processing.rateLimit,
        );

        // Initialize concurrent processor with configuration
        this.concurrentProcessor = createImageProcessor<Flash, void>({
            maxConcurrency: config.processing.concurrency,
            rateLimiter: this.processingRateLimiter,
            retryAttempts: config.processing.retries,
            retryDelay: 2000,
            onProgress: (completed, total, active) => {
                this.activeProcessing = active;
                if (completed % 50 === 0) {
                    // Log progress every 50 items
                    console.log(
                        `[EnhancedFlashConsumer] Progress: ${completed}/${total} (${active} active)`,
                    );
                }
            },
            onError: (error, flash) => {
                console.error(
                    `[EnhancedFlashConsumer] Processing failed for flash ${(flash as Flash)?.flash_id}:`,
                    error.message,
                );
            },
        });

        // Setup circuit breakers
        this.setupCircuitBreakers();

        // Start monitoring systems
        this.startMonitoring();

        // Setup memory optimization
        memoryOptimizer.startMonitoring(30000);
        memoryOptimizer.setGcThreshold(config.processing.gcThreshold);
    }

    private setupCircuitBreakers(): void {
        // Create circuit breakers for different operations
        circuitBreakerRegistry.create(
            "image_download",
            this.downloadImage.bind(this),
            {
                failureThreshold: 10,
                resetTimeout: 60000,
                monitoringPeriod: 300000,
            },
        );

        circuitBreakerRegistry.create(
            "ipfs_upload",
            this.uploadToIPFS.bind(this),
            {
                failureThreshold: 15,
                resetTimeout: 120000,
                monitoringPeriod: 300000,
            },
        );

        console.log("[EnhancedFlashConsumer] Circuit breakers configured");
    }

    private startMonitoring(): void {
        const monitoringConfig = configManager.getMonitoring();

        if (monitoringConfig.enabled) {
            this.healthChecker.start();

            // Start batch monitoring
            this.batchResultCheckInterval = setInterval(() => {
                this.monitorBatchResults();
            }, 30000);

            // Log system status periodically
            setInterval(() => {
                this.logSystemStatus();
            }, monitoringConfig.interval);

            console.log("[EnhancedFlashConsumer] Monitoring systems started");
        }
    }

    private async monitorBatchResults(): Promise<void> {
        try {
            if (this.batchUpdater.getBatchSize() > 0) {
                const result = await this.batchUpdater.forceFlush();
                await this.handleBatchUpdateResult(result);
            }
        } catch (error) {
            console.error(
                "[EnhancedFlashConsumer] Batch monitoring error:",
                error,
            );
            metricsCollector.incrementCounter("batch.monitoring.errors");
        }
    }

    private logSystemStatus(): void {
        const health = this.healthChecker.getLastHealthCheck();
        const metrics = metricsCollector.getPerformanceMetrics();
        const memory = memoryOptimizer.getMemoryStats();
        const circuitBreakers = circuitBreakerRegistry.getHealthStatus();

        console.log(
            `[EnhancedFlashConsumer] Status: Processing=${metrics.processingRate.toFixed(1)}/s, Memory=${memory.usage.toFixed(1)}%, Errors=${metrics.errorRate.toFixed(1)}%, Health=${health?.status || "unknown"}, Circuits=${circuitBreakers.healthy}/${circuitBreakers.total}`,
        );
    }

    protected shouldRequeueOnFailure(error: Error): boolean {
        const errorMsg = error.message.toLowerCase();

        // Don't requeue permanent errors
        if (
            errorMsg.includes("flash already has ipfs") ||
            errorMsg.includes("already processed") ||
            errorMsg.includes("duplicate") ||
            errorMsg.includes("circuit breaker is open")
        ) {
            return false;
        }

        // Don't requeue if we're shutting down
        if (this.shutdownRequested) {
            return false;
        }

        return true;
    }

    protected async handleMessage(msg: ConsumeMessage): Promise<void> {
        const startTime = Date.now();
        const content = msg.content.toString();
        const flash: Flash = JSON.parse(content);

        try {
            metricsCollector.incrementCounter("messages.received");

            // Check if already processed
            if (await this.isAlreadyProcessed(flash.flash_id)) {
                metricsCollector.incrementCounter("messages.skipped");
                return;
            }

            // Apply rate limiting
            await this.processingRateLimiter.waitIfNeeded();

            // Download image with circuit breaker
            const imageData = await this.downloadImage(flash);

            // Upload to IPFS with circuit breaker
            const ipfsHash = await this.uploadToIPFS(imageData);

            // Add to batch updater
            if (ipfsHash) {
                await this.batchUpdater.addUpdate(flash.flash_id, ipfsHash);
                metricsCollector.recordProcessing("flash", startTime, true);

                // Log successful processing occasionally
                if (flash.flash_id % 100 === 0) {
                    console.log(
                        `[EnhancedFlashConsumer] Successfully processed flash ${flash.flash_id}: ${ipfsHash}`,
                    );
                }
            }

            // Reset failure counters on success
            this.consecutiveApiFailures = 0;
        } catch (error) {
            metricsCollector.recordProcessing("flash", startTime, false);

            const errorMsg =
                error instanceof Error ? error.message : "Unknown error";
            console.error(
                `[EnhancedFlashConsumer] Processing failed for flash ${flash.flash_id}: ${errorMsg}`,
            );

            this.consecutiveApiFailures++;

            // Emergency shutdown on too many failures
            if (this.consecutiveApiFailures >= 50) {
                console.error(
                    `[EnhancedFlashConsumer] CRITICAL: Too many consecutive failures (${this.consecutiveApiFailures}) - initiating shutdown`,
                );
                process.exit(1);
            }

            throw error;
        }
    }

    private async isAlreadyProcessed(flashId: number): Promise<boolean> {
        try {
            const result = await databasePool.query(
                "SELECT ipfs_cid FROM flashes WHERE flash_id = $1",
                [flashId],
            );

            return result.rows.length > 0 && result.rows[0].ipfs_cid;
        } catch (error) {
            console.error(
                `[EnhancedFlashConsumer] Database check failed for flash ${flashId}:`,
                error,
            );
            return false; // Assume not processed if we can't check
        }
    }

    private async downloadImage(flash: Flash): Promise<Buffer> {
        const imageUrl = BASE_URL + flash.img;
        const headers = this.getRealisticHeaders();

        const { agent, proxy } = proxyRotator.createProxyAgent(imageUrl);

        try {
            const response = await axios.get(imageUrl, {
                responseType: "arraybuffer",
                headers,
                timeout: configManager.get().processing.timeout,
                maxRedirects: 5,
                validateStatus: (status) => status < 400,
                ...(imageUrl.startsWith("https://")
                    ? { httpsAgent: agent }
                    : { httpAgent: agent }),
            });

            return Buffer.from(response.data);
        } catch (error) {
            proxyRotator.handleProxyFailure(proxy, error as Error);

            const errorMsg =
                error instanceof Error ? error.message : "Unknown error";
            throw new Error(`Image download failed: ${errorMsg}`);
        }
    }

    private async uploadToIPFS(imageData: Buffer): Promise<string> {
        // Generate filename from flash data
        const filename = `image_${Date.now()}.jpg`;

        const file = new File([new Uint8Array(imageData)], filename, {
            type: "image/jpeg",
        });
        const formData = new FormData();
        formData.append("file", file);
        formData.append("pinataMetadata", JSON.stringify({ name: filename }));

        const response = await axios.post(
            "https://api.pinata.cloud/pinning/pinFileToIPFS",
            formData,
            {
                headers: {
                    Authorization: `Bearer ${PINATA_JWT}`,
                    "Content-Type": "multipart/form-data",
                },
                timeout: configManager.get().pinata.timeout,
                validateStatus: (status) => status < 500,
            },
        );

        if (response.status === 429) {
            const retryAfter = response.headers["retry-after"] || "60";
            throw new Error(
                `Rate limited by Pinata API - retry after: ${retryAfter}s`,
            );
        }

        if (response.status >= 400) {
            throw new Error(
                `Pinata API error: ${response.status} ${response.statusText}`,
            );
        }

        return response.data.IpfsHash;
    }

    private getRealisticHeaders(): Record<string, string> {
        const userAgent =
            USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const referrer =
            REFERRERS[Math.floor(Math.random() * REFERRERS.length)];

        return {
            "User-Agent": userAgent,
            Accept: "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            DNT: "1",
            Connection: "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "image",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "cross-site",
            "Cache-Control": "no-cache",
            Referer: referrer,
        };
    }

    private async handleBatchUpdateResult(
        result: BatchUpdateResult,
    ): Promise<void> {
        if (result.failed.length > 0) {
            metricsCollector.incrementCounter(
                "batch.failed",
                result.failed.length,
            );

            for (const failedUpdate of result.failed) {
                try {
                    const flashMessage =
                        await this.createFlashMessageFromUpdate(failedUpdate);
                    if (flashMessage) {
                        await this.requeueMessage(flashMessage);
                    }
                } catch (error) {
                    console.error(
                        `[EnhancedFlashConsumer] Failed to requeue flash ${failedUpdate.flash_id}:`,
                        error,
                    );
                }
            }
        }

        if (result.successful > 0) {
            metricsCollector.incrementCounter(
                "batch.successful",
                result.successful,
            );
        }
    }

    private async createFlashMessageFromUpdate(update: {
        flash_id: number;
        ipfs_cid: string;
    }): Promise<Flash | null> {
        try {
            const result = await databasePool.query(
                "SELECT * FROM flashes WHERE flash_id = $1",
                [update.flash_id],
            );

            if (result.rows.length > 0) {
                const flash = result.rows[0];
                return {
                    flash_id: flash.flash_id,
                    img: flash.img,
                    city: flash.city,
                    text: flash.text || "",
                    player: flash.player,
                    timestamp: Math.floor(flash.timestamp.getTime() / 1000),
                    flash_count: flash.flash_count || "",
                    ipfs_cid: "",
                };
            }
        } catch (error) {
            console.error(
                `[EnhancedFlashConsumer] Error creating flash message for ${update.flash_id}:`,
                error,
            );
        }
        return null;
    }

    private async requeueMessage(flash: Flash): Promise<void> {
        // TODO: Implement actual RabbitMQ publisher functionality
        console.log(
            `[EnhancedFlashConsumer] Requeuing flash ${flash.flash_id} for retry`,
        );
    }

    public async shutdown(): Promise<void> {
        this.shutdownRequested = true;
        console.log("[EnhancedFlashConsumer] Starting graceful shutdown...");

        try {
            // Stop monitoring
            if (this.batchResultCheckInterval) {
                clearInterval(this.batchResultCheckInterval);
            }

            this.healthChecker.stop();
            memoryOptimizer.stopMonitoring();

            // Wait for active processing to complete
            while (this.activeProcessing > 0) {
                console.log(
                    `[EnhancedFlashConsumer] Waiting for ${this.activeProcessing} active processes to complete...`,
                );
                await new Promise((resolve) => setTimeout(resolve, 1000));
            }

            // Shutdown batch updater
            await this.batchUpdater.shutdown();
            console.log(
                "[EnhancedFlashConsumer] Batch updater shutdown complete",
            );

            // Shutdown database pool
            await databasePool.shutdown();
            console.log(
                "[EnhancedFlashConsumer] Database pool shutdown complete",
            );

            // Clean up metrics and memory optimizer
            metricsCollector.shutdown();
            memoryOptimizer.cleanup();

            console.log("[EnhancedFlashConsumer] Graceful shutdown completed");
        } catch (error) {
            console.error(
                "[EnhancedFlashConsumer] Error during shutdown:",
                error,
            );
            throw error;
        }
    }
}

// Main execution
(async () => {
    const consumer = new EnhancedFlashConsumer();
    const testMode = process.env.TEST_MODE === "true";

    console.log(
        `[EnhancedFlashConsumer] Starting in ${testMode ? "TEST" : "PRODUCTION"} mode`,
    );
    console.log(
        `[EnhancedFlashConsumer] Configuration: ${JSON.stringify(configManager.getSummary())}`,
    );

    // Start health API
    try {
        await startHealthApi();
    } catch (error) {
        console.error(
            "[EnhancedFlashConsumer] Failed to start health API:",
            error,
        );
    }

    // Initialize proxy system
    await proxyRotator.initialize();

    if (proxyRotator.getTotalProxyCount() > 0) {
        console.log(
            `[EnhancedFlashConsumer] Proxy system: ${proxyRotator.getProxyCount()}/${proxyRotator.getTotalProxyCount()} proxies active`,
        );
    } else {
        console.log(
            "[EnhancedFlashConsumer] No proxy configured - using direct connections",
        );
    }

    // Graceful shutdown handlers
    const shutdown = async (signal: string) => {
        console.log(
            `\n[EnhancedFlashConsumer] Received ${signal}, shutting down gracefully...`,
        );

        try {
            await consumer.shutdown();
            process.exit(0);
        } catch (error) {
            console.error(`[EnhancedFlashConsumer] Shutdown error:`, error);
            process.exit(1);
        }
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    // Handle unhandled rejections
    process.on("unhandledRejection", (reason, promise) => {
        console.error(
            "[EnhancedFlashConsumer] Unhandled rejection at:",
            promise,
            "reason:",
            reason,
        );
        process.exit(1);
    });

    await consumer.startConsuming(testMode);
})();
