import { EventEmitter } from "events";
import { metricsCollector } from "./metrics";

export interface HealthStatus {
    status: "healthy" | "degraded" | "unhealthy";
    timestamp: number;
    uptime: number;
    checks: {
        database: HealthCheckResult;
        pinata: HealthCheckResult;
        memory: HealthCheckResult;
        disk: HealthCheckResult;
        processing: HealthCheckResult;
    };
    summary: {
        total: number;
        passing: number;
        failing: number;
    };
}

export interface HealthCheckResult {
    status: "pass" | "warn" | "fail";
    message: string;
    duration: number;
    details?: Record<string, any>;
}

export class HealthChecker extends EventEmitter {
    private checkInterval?: NodeJS.Timeout;
    private isRunning: boolean = false;
    private lastCheck?: HealthStatus;

    constructor(
        private dbPool: any,
        private checkIntervalMs: number = 30000,
    ) {
        super();
    }

    /**
     * Start periodic health checking
     */
    start(): void {
        if (this.isRunning) return;

        this.isRunning = true;
        this.runHealthCheck(); // Run immediately

        this.checkInterval = setInterval(() => {
            this.runHealthCheck();
        }, this.checkIntervalMs);

        // Health monitoring started - shown in startup banner
    }

    /**
     * Stop health checking
     */
    stop(): void {
        if (this.checkInterval) {
            clearInterval(this.checkInterval);
            this.checkInterval = undefined;
        }
        this.isRunning = false;
        console.log("[HealthChecker] Stopped health monitoring");
    }

    /**
     * Run comprehensive health check
     */
    async runHealthCheck(): Promise<HealthStatus> {
        const startTime = Date.now();

        try {
            // Health checks run silently - results logged in status line

            const [database, pinata, memory, disk, processing] =
                await Promise.allSettled([
                    this.checkDatabase(),
                    this.checkPinata(),
                    this.checkMemory(),
                    this.checkDisk(),
                    this.checkProcessing(),
                ]);

            const checks = {
                database: this.getResultFromSettled(database),
                pinata: this.getResultFromSettled(pinata),
                memory: this.getResultFromSettled(memory),
                disk: this.getResultFromSettled(disk),
                processing: this.getResultFromSettled(processing),
            };

            const summary = this.calculateSummary(checks);
            const overallStatus = this.determineOverallStatus(checks);

            const healthStatus: HealthStatus = {
                status: overallStatus,
                timestamp: Date.now(),
                uptime: Math.floor(process.uptime()),
                checks,
                summary,
            };

            this.lastCheck = healthStatus;
            this.emit("healthCheck", healthStatus);

            // Log status changes
            if (overallStatus !== "healthy") {
                console.warn(
                    `[HealthChecker] System status: ${overallStatus.toUpperCase()}`,
                );
                this.logFailingChecks(checks);
            }

            // Record metrics
            metricsCollector.recordMetric(
                "health.check_duration",
                Date.now() - startTime,
            );
            metricsCollector.recordMetric(
                "health.passing_checks",
                summary.passing,
            );
            metricsCollector.recordMetric(
                "health.failing_checks",
                summary.failing,
            );

            return healthStatus;
        } catch (error) {
            console.error("[HealthChecker] Health check failed:", error);

            const failedStatus: HealthStatus = {
                status: "unhealthy",
                timestamp: Date.now(),
                uptime: Math.floor(process.uptime()),
                checks: {
                    database: {
                        status: "fail",
                        message: "Health check failed",
                        duration: 0,
                    },
                    pinata: {
                        status: "fail",
                        message: "Health check failed",
                        duration: 0,
                    },
                    memory: {
                        status: "fail",
                        message: "Health check failed",
                        duration: 0,
                    },
                    disk: {
                        status: "fail",
                        message: "Health check failed",
                        duration: 0,
                    },
                    processing: {
                        status: "fail",
                        message: "Health check failed",
                        duration: 0,
                    },
                },
                summary: { total: 5, passing: 0, failing: 5 },
            };

            this.lastCheck = failedStatus;
            return failedStatus;
        }
    }

    /**
     * Check database connectivity and performance
     */
    private async checkDatabase(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            // Test basic connectivity
            const result = await this.dbPool.query("SELECT 1 as test");
            if (!result.rows || result.rows[0]?.test !== 1) {
                throw new Error("Database query returned unexpected result");
            }

            // Check pool status
            const poolStats = {
                total: this.dbPool.totalCount || 0,
                idle: this.dbPool.idleCount || 0,
                waiting: this.dbPool.waitingCount || 0,
            };

            const duration = Date.now() - startTime;

            // Warning thresholds
            if (duration > 5000) {
                return {
                    status: "warn",
                    message: `Database responding slowly (${duration}ms)`,
                    duration,
                    details: { poolStats, queryTime: duration },
                };
            }

            if (poolStats.waiting > 0) {
                return {
                    status: "warn",
                    message: `Database pool has ${poolStats.waiting} waiting connections`,
                    duration,
                    details: { poolStats },
                };
            }

            return {
                status: "pass",
                message: `Database healthy (${duration}ms)`,
                duration,
                details: { poolStats },
            };
        } catch (error) {
            return {
                status: "fail",
                message: `Database check failed: ${(error as Error).message}`,
                duration: Date.now() - startTime,
            };
        }
    }

    /**
     * Check Pinata API availability
     */
    private async checkPinata(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            // Simple test - this would need to be implemented based on your Pinata usage
            // For now, we'll check if the API key is available
            const apiKey = process.env.PINATA_API_KEY;
            const apiSecret = process.env.PINATA_API_SECRET;

            if (!apiKey || !apiSecret) {
                return {
                    status: "fail",
                    message: "Pinata credentials not configured",
                    duration: Date.now() - startTime,
                };
            }

            // You could add an actual API call here to test connectivity
            // const response = await axios.get('https://api.pinata.cloud/data/testAuthentication', {
            //   headers: { pinata_api_key: apiKey, pinata_secret_api_key: apiSecret }
            // });

            const duration = Date.now() - startTime;

            return {
                status: "pass",
                message: `Pinata configuration verified (${duration}ms)`,
                duration,
                details: { configured: true },
            };
        } catch (error) {
            return {
                status: "fail",
                message: `Pinata check failed: ${(error as Error).message}`,
                duration: Date.now() - startTime,
            };
        }
    }

    /**
     * Check memory usage
     */
    private async checkMemory(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            const memUsage = process.memoryUsage();
            const usage = {
                heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024), // MB
                heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024), // MB
                external: Math.round(memUsage.external / 1024 / 1024), // MB
                rss: Math.round(memUsage.rss / 1024 / 1024), // MB
                usage: memUsage.heapUsed / memUsage.heapTotal,
            };

            const duration = Date.now() - startTime;

            // Critical thresholds
            if (usage.usage > 0.999) {
                return {
                    status: "fail",
                    message: `Critical memory usage: ${(usage.usage * 100).toFixed(1)}%`,
                    duration,
                    details: usage,
                };
            }

            // Warning thresholds
            if (usage.usage > 0.99) {
                return {
                    status: "warn",
                    message: `High memory usage: ${(usage.usage * 100).toFixed(1)}%`,
                    duration,
                    details: usage,
                };
            }

            return {
                status: "pass",
                message: `Memory usage normal: ${(usage.usage * 100).toFixed(1)}%`,
                duration,
                details: usage,
            };
        } catch (error) {
            return {
                status: "fail",
                message: `Memory check failed: ${(error as Error).message}`,
                duration: Date.now() - startTime,
            };
        }
    }

    /**
     * Check disk space
     */
    private async checkDisk(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            const fs = require("fs");
            const stats = await fs.promises.statSync(process.cwd());

            // Note: Getting actual disk space requires platform-specific commands
            // This is a simplified check
            const duration = Date.now() - startTime;

            return {
                status: "pass",
                message: `Disk access verified (${duration}ms)`,
                duration,
                details: { accessible: true },
            };
        } catch (error) {
            return {
                status: "fail",
                message: `Disk check failed: ${(error as Error).message}`,
                duration: Date.now() - startTime,
            };
        }
    }

    /**
     * Check processing performance
     */
    private async checkProcessing(): Promise<HealthCheckResult> {
        const startTime = Date.now();

        try {
            const metrics = metricsCollector.getPerformanceMetrics();
            const duration = Date.now() - startTime;

            // Check error rate
            if (metrics.errorRate > 10) {
                return {
                    status: "fail",
                    message: `High error rate: ${metrics.errorRate.toFixed(1)}%`,
                    duration,
                    details: metrics,
                };
            }

            if (metrics.errorRate > 5) {
                return {
                    status: "warn",
                    message: `Elevated error rate: ${metrics.errorRate.toFixed(1)}%`,
                    duration,
                    details: metrics,
                };
            }

            // Check processing time
            if (metrics.avgProcessingTime > 30000) {
                return {
                    status: "warn",
                    message: `Slow processing: ${metrics.avgProcessingTime}ms avg`,
                    duration,
                    details: metrics,
                };
            }

            return {
                status: "pass",
                message: `Processing healthy: ${metrics.processingRate.toFixed(1)}/s rate`,
                duration,
                details: metrics,
            };
        } catch (error) {
            return {
                status: "fail",
                message: `Processing check failed: ${(error as Error).message}`,
                duration: Date.now() - startTime,
            };
        }
    }

    /**
     * Extract result from Promise.allSettled
     */
    private getResultFromSettled(
        settled: PromiseSettledResult<HealthCheckResult>,
    ): HealthCheckResult {
        if (settled.status === "fulfilled") {
            return settled.value;
        } else {
            return {
                status: "fail",
                message: `Check failed: ${settled.reason}`,
                duration: 0,
            };
        }
    }

    /**
     * Calculate summary statistics
     */
    private calculateSummary(
        checks: HealthStatus["checks"],
    ): HealthStatus["summary"] {
        const checkResults = Object.values(checks);
        const total = checkResults.length;
        const passing = checkResults.filter(
            (check) => check.status === "pass",
        ).length;
        const failing = checkResults.filter(
            (check) => check.status === "fail",
        ).length;

        return { total, passing, failing };
    }

    /**
     * Determine overall system status
     */
    private determineOverallStatus(
        checks: HealthStatus["checks"],
    ): HealthStatus["status"] {
        const checkResults = Object.values(checks);

        if (checkResults.some((check) => check.status === "fail")) {
            return "unhealthy";
        }

        if (checkResults.some((check) => check.status === "warn")) {
            return "degraded";
        }

        return "healthy";
    }

    /**
     * Log failing health checks
     */
    private logFailingChecks(checks: HealthStatus["checks"]): void {
        Object.entries(checks).forEach(([name, result]) => {
            if (result.status !== "pass") {
                console.warn(
                    `[HealthChecker] ${name}: ${result.status.toUpperCase()} - ${result.message}`,
                );
            }
        });
    }

    /**
     * Get the last health check result
     */
    getLastHealthCheck(): HealthStatus | null {
        return this.lastCheck || null;
    }

    /**
     * Manual health check trigger
     */
    async checkHealth(): Promise<HealthStatus> {
        return this.runHealthCheck();
    }
}

// Factory function
export function createHealthChecker(
    dbPool: any,
    intervalMs?: number,
): HealthChecker {
    return new HealthChecker(dbPool, intervalMs);
}
