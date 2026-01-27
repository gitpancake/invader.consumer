import { EventEmitter } from 'events';

export interface MetricValue {
  value: number;
  timestamp: number;
  tags?: Record<string, string>;
}

export interface PerformanceMetrics {
  processingRate: number; // items per second
  avgProcessingTime: number; // milliseconds
  errorRate: number; // percentage
  queueDepth: number;
  memoryUsage: number; // MB
  uptime: number; // seconds
}

export interface Alert {
  id: string;
  level: 'warning' | 'critical';
  message: string;
  timestamp: number;
  metric: string;
  value: number;
  threshold: number;
}

export class MetricsCollector extends EventEmitter {
  private metrics: Map<string, MetricValue[]> = new Map();
  private counters: Map<string, number> = new Map();
  private alerts: Alert[] = [];
  private thresholds: Map<string, { warning: number; critical: number }> = new Map();
  private retentionPeriod: number = 24 * 60 * 60 * 1000; // 24 hours
  private cleanupInterval?: NodeJS.Timeout;

  constructor() {
    super();
    this.setupDefaultThresholds();
    this.startCleanup();
  }

  /**
   * Record a metric value
   */
  recordMetric(name: string, value: number, tags?: Record<string, string>): void {
    if (!this.metrics.has(name)) {
      this.metrics.set(name, []);
    }

    const metric: MetricValue = {
      value,
      timestamp: Date.now(),
      tags
    };

    this.metrics.get(name)!.push(metric);
    this.checkThresholds(name, value);
    this.emit('metric', { name, ...metric });
  }

  /**
   * Increment a counter
   */
  incrementCounter(name: string, delta: number = 1): void {
    const current = this.counters.get(name) || 0;
    this.counters.set(name, current + delta);
    this.emit('counter', { name, value: current + delta });
  }

  /**
   * Record processing time and update rate metrics
   */
  recordProcessing(operation: string, startTime: number, success: boolean = true): void {
    const processingTime = Date.now() - startTime;
    
    this.recordMetric(`${operation}.processing_time`, processingTime);
    this.incrementCounter(`${operation}.total`);
    
    if (success) {
      this.incrementCounter(`${operation}.success`);
    } else {
      this.incrementCounter(`${operation}.error`);
    }

    // Update rate metrics every 10 operations
    const total = this.counters.get(`${operation}.total`) || 0;
    if (total % 10 === 0) {
      this.updateRateMetrics(operation);
    }
  }

  /**
   * Get current performance metrics summary
   */
  getPerformanceMetrics(): PerformanceMetrics {
    const now = Date.now();
    const fiveMinutesAgo = now - (5 * 60 * 1000);

    // Calculate processing rate (last 5 minutes)
    const recentProcessing = this.getMetricsSince('image.processing_time', fiveMinutesAgo);
    const processingRate = recentProcessing.length / (5 * 60); // per second

    // Average processing time (last 5 minutes)
    const avgProcessingTime = recentProcessing.length > 0 
      ? recentProcessing.reduce((sum, m) => sum + m.value, 0) / recentProcessing.length
      : 0;

    // Error rate calculation
    const totalRequests = this.counters.get('image.total') || 0;
    const errorRequests = this.counters.get('image.error') || 0;
    const errorRate = totalRequests > 0 ? (errorRequests / totalRequests) * 100 : 0;

    // Memory usage
    const memUsage = process.memoryUsage();
    const memoryUsage = Math.round(memUsage.heapUsed / 1024 / 1024);

    // Queue depth (if available)
    const queueDepthMetrics = this.getLatestMetric('queue.depth');
    const queueDepth = queueDepthMetrics?.value || 0;

    // Uptime
    const uptime = Math.floor(process.uptime());

    return {
      processingRate,
      avgProcessingTime,
      errorRate,
      queueDepth,
      memoryUsage,
      uptime
    };
  }

  /**
   * Get metrics since a timestamp
   */
  private getMetricsSince(name: string, since: number): MetricValue[] {
    const metrics = this.metrics.get(name) || [];
    return metrics.filter(m => m.timestamp >= since);
  }

  /**
   * Get the latest metric value
   */
  private getLatestMetric(name: string): MetricValue | null {
    const metrics = this.metrics.get(name) || [];
    return metrics.length > 0 ? metrics[metrics.length - 1] : null;
  }

  /**
   * Update rate metrics for an operation
   */
  private updateRateMetrics(operation: string): void {
    const now = Date.now();
    const oneMinuteAgo = now - (60 * 1000);

    // Count operations in the last minute
    const recentOps = this.getMetricsSince(`${operation}.processing_time`, oneMinuteAgo);
    const rate = recentOps.length / 60; // operations per second

    this.recordMetric(`${operation}.rate`, rate);
  }

  /**
   * Set threshold for metric alerts
   */
  setThreshold(metric: string, warning: number, critical: number): void {
    this.thresholds.set(metric, { warning, critical });
  }

  /**
   * Check if metric value exceeds thresholds
   */
  private checkThresholds(name: string, value: number): void {
    const threshold = this.thresholds.get(name);
    if (!threshold) return;

    let alertLevel: 'warning' | 'critical' | null = null;
    let thresholdValue: number = 0;

    if (value >= threshold.critical) {
      alertLevel = 'critical';
      thresholdValue = threshold.critical;
    } else if (value >= threshold.warning) {
      alertLevel = 'warning';
      thresholdValue = threshold.warning;
    }

    if (alertLevel) {
      const alert: Alert = {
        id: `${name}-${Date.now()}`,
        level: alertLevel,
        message: `Metric ${name} is ${value}, exceeding ${alertLevel} threshold of ${thresholdValue}`,
        timestamp: Date.now(),
        metric: name,
        value,
        threshold: thresholdValue
      };

      this.alerts.push(alert);
      this.emit('alert', alert);
      
      console.warn(`[MetricsCollector] ${alertLevel.toUpperCase()}: ${alert.message}`);
    }
  }

  /**
   * Setup default thresholds
   */
  private setupDefaultThresholds(): void {
    // Processing time thresholds (milliseconds)
    this.setThreshold('image.processing_time', 30000, 60000); // 30s warning, 60s critical
    this.setThreshold('pinata.processing_time', 15000, 30000); // 15s warning, 30s critical
    
    // Memory usage thresholds (MB)
    this.setThreshold('memory.heap_used', 512, 1024); // 512MB warning, 1GB critical
    
    // Error rate thresholds (percentage)
    this.setThreshold('error_rate', 5, 10); // 5% warning, 10% critical
    
    // Queue depth thresholds
    this.setThreshold('queue.depth', 1000, 5000); // 1k warning, 5k critical
  }

  /**
   * Get recent alerts
   */
  getRecentAlerts(since?: number): Alert[] {
    const cutoff = since || Date.now() - (60 * 60 * 1000); // Last hour by default
    return this.alerts.filter(alert => alert.timestamp >= cutoff);
  }

  /**
   * Clear old alerts
   */
  clearOldAlerts(): void {
    const oneHourAgo = Date.now() - (60 * 60 * 1000);
    this.alerts = this.alerts.filter(alert => alert.timestamp >= oneHourAgo);
  }

  /**
   * Get metric statistics
   */
  getMetricStats(name: string, period?: number): {
    count: number;
    avg: number;
    min: number;
    max: number;
    latest: number;
  } | null {
    const cutoff = period ? Date.now() - period : 0;
    const metrics = this.getMetricsSince(name, cutoff);
    
    if (metrics.length === 0) return null;

    const values = metrics.map(m => m.value);
    
    return {
      count: values.length,
      avg: values.reduce((a, b) => a + b, 0) / values.length,
      min: Math.min(...values),
      max: Math.max(...values),
      latest: values[values.length - 1]
    };
  }

  /**
   * Start periodic cleanup of old metrics
   */
  private startCleanup(): void {
    this.cleanupInterval = setInterval(() => {
      this.cleanupOldMetrics();
      this.clearOldAlerts();
    }, 60 * 60 * 1000); // Every hour
  }

  /**
   * Clean up old metrics
   */
  private cleanupOldMetrics(): void {
    const cutoff = Date.now() - this.retentionPeriod;
    let totalCleaned = 0;

    this.metrics.forEach((metrics, name) => {
      const before = metrics.length;
      const filtered = metrics.filter(m => m.timestamp >= cutoff);
      this.metrics.set(name, filtered);
      totalCleaned += before - filtered.length;
    });

    if (totalCleaned > 0) {
      console.log(`[MetricsCollector] Cleaned up ${totalCleaned} old metric entries`);
    }
  }

  /**
   * Export metrics for external monitoring systems
   */
  exportMetrics(): {
    metrics: Record<string, MetricValue[]>;
    counters: Record<string, number>;
    performance: PerformanceMetrics;
    alerts: Alert[];
  } {
    return {
      metrics: Object.fromEntries(this.metrics),
      counters: Object.fromEntries(this.counters),
      performance: this.getPerformanceMetrics(),
      alerts: this.getRecentAlerts()
    };
  }

  /**
   * Shutdown cleanup
   */
  shutdown(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.removeAllListeners();
  }
}

// Export singleton instance
export const metricsCollector = new MetricsCollector();