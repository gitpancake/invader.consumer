import { EventEmitter } from "events";
import { metricsCollector } from "../monitoring/metrics";

export interface CircuitBreakerOptions {
    failureThreshold?: number;
    resetTimeout?: number;
    monitoringPeriod?: number;
    expectedFailureRate?: number;
    minimumRequests?: number;
}

export enum CircuitBreakerState {
    CLOSED = "CLOSED",
    OPEN = "OPEN",
    HALF_OPEN = "HALF_OPEN",
}

export interface CircuitBreakerStats {
    state: CircuitBreakerState;
    failureCount: number;
    successCount: number;
    requestCount: number;
    failureRate: number;
    nextRetryTime?: number;
}

export class CircuitBreaker<T> extends EventEmitter {
    private state: CircuitBreakerState = CircuitBreakerState.CLOSED;
    private failureCount: number = 0;
    private successCount: number = 0;
    private requestCount: number = 0;
    private lastFailureTime?: number;
    private nextRetryTime?: number;
    private readonly options: Required<CircuitBreakerOptions>;
    private readonly name: string;

    constructor(
        name: string,
        private fn: (...args: any[]) => Promise<T>,
        options: CircuitBreakerOptions = {},
    ) {
        super();
        this.name = name;
        this.options = {
            failureThreshold: 5,
            resetTimeout: 60000, // 1 minute
            monitoringPeriod: 300000, // 5 minutes
            expectedFailureRate: 0.1, // 10%
            minimumRequests: 10,
            ...options,
        };

        // Reset counters periodically
        setInterval(() => {
            this.resetCounters();
        }, this.options.monitoringPeriod);
    }

    /**
     * Execute the wrapped function with circuit breaker logic
     */
    async execute(...args: any[]): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const startTime = Date.now();

            // Check if circuit is open
            if (this.state === CircuitBreakerState.OPEN) {
                if (this.canAttemptReset()) {
                    this.state = CircuitBreakerState.HALF_OPEN;
                    console.log(
                        `[CircuitBreaker:${this.name}] State changed to HALF_OPEN`,
                    );
                    this.emit("stateChange", this.state);
                } else {
                    const error = new Error(
                        `Circuit breaker is OPEN. Next retry: ${new Date(this.nextRetryTime!)}`,
                    );
                    this.recordMetrics(false, Date.now() - startTime);
                    return reject(error);
                }
            }

            // Execute the function
            this.fn(...args)
                .then((result: T) => {
                    this.onSuccess();
                    this.recordMetrics(true, Date.now() - startTime);
                    resolve(result);
                })
                .catch((error: Error) => {
                    this.onFailure();
                    this.recordMetrics(false, Date.now() - startTime);
                    reject(error);
                });
        });
    }

    /**
     * Handle successful execution
     */
    private onSuccess(): void {
        this.successCount++;
        this.requestCount++;

        // Reset failure count on success
        if (this.state === CircuitBreakerState.HALF_OPEN) {
            this.state = CircuitBreakerState.CLOSED;
            this.failureCount = 0;
            console.log(
                `[CircuitBreaker:${this.name}] State changed to CLOSED after successful half-open request`,
            );
            this.emit("stateChange", this.state);
        } else if (this.state === CircuitBreakerState.CLOSED) {
            // Reset failure count after successful requests
            this.failureCount = Math.max(0, this.failureCount - 1);
        }
    }

    /**
     * Handle failed execution
     */
    private onFailure(): void {
        this.failureCount++;
        this.requestCount++;
        this.lastFailureTime = Date.now();

        // Check if we should open the circuit
        if (this.shouldOpenCircuit()) {
            this.state = CircuitBreakerState.OPEN;
            this.nextRetryTime = Date.now() + this.options.resetTimeout;
            console.warn(
                `[CircuitBreaker:${this.name}] Circuit OPENED due to ${this.failureCount} failures`,
            );
            this.emit("stateChange", this.state);
            this.emit("circuitOpen", this.getStats());
        } else if (this.state === CircuitBreakerState.HALF_OPEN) {
            // Go back to open if half-open attempt fails
            this.state = CircuitBreakerState.OPEN;
            this.nextRetryTime = Date.now() + this.options.resetTimeout;
            console.warn(
                `[CircuitBreaker:${this.name}] Returned to OPEN state after half-open failure`,
            );
            this.emit("stateChange", this.state);
        }
    }

    /**
     * Check if circuit should be opened
     */
    private shouldOpenCircuit(): boolean {
        // Must have minimum requests before considering opening
        if (this.requestCount < this.options.minimumRequests) {
            return false;
        }

        // Check failure count threshold
        if (this.failureCount >= this.options.failureThreshold) {
            return true;
        }

        // Check failure rate
        const currentFailureRate =
            this.requestCount > 0 ? this.failureCount / this.requestCount : 0;
        return currentFailureRate > this.options.expectedFailureRate;
    }

    /**
     * Check if we can attempt to reset (half-open)
     */
    private canAttemptReset(): boolean {
        return (
            this.nextRetryTime !== undefined && Date.now() >= this.nextRetryTime
        );
    }

    /**
     * Reset failure and success counters
     */
    private resetCounters(): void {
        if (this.state === CircuitBreakerState.CLOSED) {
            const oldFailureCount = this.failureCount;
            const oldRequestCount = this.requestCount;

            this.failureCount = 0;
            this.successCount = 0;
            this.requestCount = 0;

            if (oldRequestCount > 0) {
                console.log(
                    `[CircuitBreaker:${this.name}] Reset counters - Previous period: ${oldRequestCount} requests, ${oldFailureCount} failures`,
                );
            }
        }
    }

    /**
     * Record metrics for monitoring
     */
    private recordMetrics(success: boolean, duration: number): void {
        metricsCollector.recordMetric(
            `circuit_breaker.${this.name}.duration`,
            duration,
        );
        metricsCollector.incrementCounter(
            `circuit_breaker.${this.name}.requests`,
        );

        if (success) {
            metricsCollector.incrementCounter(
                `circuit_breaker.${this.name}.success`,
            );
        } else {
            metricsCollector.incrementCounter(
                `circuit_breaker.${this.name}.failures`,
            );
        }

        // Record current state as metric
        const stateValue = {
            [CircuitBreakerState.CLOSED]: 0,
            [CircuitBreakerState.HALF_OPEN]: 1,
            [CircuitBreakerState.OPEN]: 2,
        }[this.state];

        metricsCollector.recordMetric(
            `circuit_breaker.${this.name}.state`,
            stateValue,
        );
    }

    /**
     * Get current circuit breaker statistics
     */
    getStats(): CircuitBreakerStats {
        const failureRate =
            this.requestCount > 0 ? this.failureCount / this.requestCount : 0;

        return {
            state: this.state,
            failureCount: this.failureCount,
            successCount: this.successCount,
            requestCount: this.requestCount,
            failureRate,
            nextRetryTime: this.nextRetryTime,
        };
    }

    /**
     * Manually reset the circuit breaker
     */
    reset(): void {
        this.state = CircuitBreakerState.CLOSED;
        this.failureCount = 0;
        this.successCount = 0;
        this.requestCount = 0;
        this.lastFailureTime = undefined;
        this.nextRetryTime = undefined;

        console.log(
            `[CircuitBreaker:${this.name}] Manual reset - state: CLOSED`,
        );
        this.emit("stateChange", this.state);
    }

    /**
     * Force open the circuit breaker
     */
    forceOpen(): void {
        this.state = CircuitBreakerState.OPEN;
        this.nextRetryTime = Date.now() + this.options.resetTimeout;

        console.log(`[CircuitBreaker:${this.name}] Forced OPEN`);
        this.emit("stateChange", this.state);
    }

    /**
     * Force close the circuit breaker
     */
    forceClose(): void {
        this.state = CircuitBreakerState.CLOSED;
        this.failureCount = 0;
        this.nextRetryTime = undefined;

        console.log(`[CircuitBreaker:${this.name}] Forced CLOSED`);
        this.emit("stateChange", this.state);
    }

    /**
     * Get circuit breaker name
     */
    getName(): string {
        return this.name;
    }

    /**
     * Check if circuit is open
     */
    isOpen(): boolean {
        return this.state === CircuitBreakerState.OPEN;
    }

    /**
     * Check if circuit is closed
     */
    isClosed(): boolean {
        return this.state === CircuitBreakerState.CLOSED;
    }

    /**
     * Check if circuit is half-open
     */
    isHalfOpen(): boolean {
        return this.state === CircuitBreakerState.HALF_OPEN;
    }
}

// Circuit breaker registry for managing multiple instances
export class CircuitBreakerRegistry {
    private breakers = new Map<string, CircuitBreaker<any>>();

    /**
     * Create or get a circuit breaker
     */
    create<T>(
        name: string,
        fn: (...args: any[]) => Promise<T>,
        options?: CircuitBreakerOptions,
    ): CircuitBreaker<T> {
        if (this.breakers.has(name)) {
            return this.breakers.get(name)!;
        }

        const breaker = new CircuitBreaker(name, fn, options);
        this.breakers.set(name, breaker);

        // Circuit breaker created silently
        return breaker;
    }

    /**
     * Get circuit breaker by name
     */
    get<T>(name: string): CircuitBreaker<T> | undefined {
        return this.breakers.get(name);
    }

    /**
     * Get all circuit breakers
     */
    getAll(): Map<string, CircuitBreaker<any>> {
        return new Map(this.breakers);
    }

    /**
     * Get stats for all circuit breakers
     */
    getAllStats(): Record<string, CircuitBreakerStats> {
        const stats: Record<string, CircuitBreakerStats> = {};

        this.breakers.forEach((breaker, name) => {
            stats[name] = breaker.getStats();
        });

        return stats;
    }

    /**
     * Reset all circuit breakers
     */
    resetAll(): void {
        this.breakers.forEach((breaker, name) => {
            breaker.reset();
            console.log(
                `[CircuitBreakerRegistry] Reset circuit breaker: ${name}`,
            );
        });
    }

    /**
     * Remove a circuit breaker
     */
    remove(name: string): boolean {
        const removed = this.breakers.delete(name);
        if (removed) {
            console.log(
                `[CircuitBreakerRegistry] Removed circuit breaker: ${name}`,
            );
        }
        return removed;
    }

    /**
     * Get health status of all circuit breakers
     */
    getHealthStatus(): {
        healthy: number;
        degraded: number;
        unhealthy: number;
        total: number;
    } {
        let healthy = 0;
        let degraded = 0;
        let unhealthy = 0;

        this.breakers.forEach((breaker) => {
            if (breaker.isClosed()) {
                healthy++;
            } else if (breaker.isHalfOpen()) {
                degraded++;
            } else {
                unhealthy++;
            }
        });

        return {
            healthy,
            degraded,
            unhealthy,
            total: this.breakers.size,
        };
    }
}

// Export singleton registry
export const circuitBreakerRegistry = new CircuitBreakerRegistry();
