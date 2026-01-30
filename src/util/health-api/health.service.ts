import { Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import { HealthChecker } from '../monitoring/health-checker';
import { metricsCollector } from '../monitoring/metrics';

@Injectable()
export class HealthService {
  private dbPool: Pool;
  private healthChecker: HealthChecker;
  private lastHealthCheck: any = null;
  private lastCheckTime: number = 0;
  private readonly CACHE_TTL = 30000;

  constructor() {
    this.dbPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 2,
      idleTimeoutMillis: 30000,
    });

    this.healthChecker = new HealthChecker(this.dbPool);
  }

  async getQuickHealth() {
    return {
      service: 'invaders-consumer',
      status: 'healthy',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
    };
  }

  async getDetailedHealth() {
    if (
      this.lastHealthCheck &&
      Date.now() - this.lastCheckTime < this.CACHE_TTL
    ) {
      return this.lastHealthCheck;
    }

    try {
      const health = await this.healthChecker.checkHealth();

      const transformed = {
        service: 'invaders-consumer',
        status: health.status,
        timestamp: new Date(health.timestamp).toISOString(),
        uptime: health.uptime,
        lastProcessed: await this.getLastProcessedTime(),
        metrics: {
          memoryUsage: this.getMemoryUsagePercent(),
          processingRate: this.getProcessingRate(),
          errorRate: this.getErrorRate(),
        },
        checks: {
          database: health.checks.database.status,
          pinata: health.checks.pinata.status,
          memory: health.checks.memory.status,
          processing: health.checks.processing.status,
        },
        responseTimes: {
          database: health.checks.database.duration,
          pinata: health.checks.pinata.duration,
        },
      };

      this.lastHealthCheck = transformed;
      this.lastCheckTime = Date.now();

      return transformed;
    } catch (error: any) {
      return {
        service: 'invaders-consumer',
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: error.message,
      };
    }
  }

  async getMetrics() {
    const memUsage = process.memoryUsage();
    const perfMetrics = metricsCollector.getPerformanceMetrics();

    return {
      process: {
        uptime: Math.floor(process.uptime()),
        pid: process.pid,
        version: process.version,
      },
      memory: {
        rss: memUsage.rss,
        heapTotal: memUsage.heapTotal,
        heapUsed: memUsage.heapUsed,
        external: memUsage.external,
      },
      processing: {
        rate: perfMetrics.processingRate,
        avgTime: perfMetrics.avgProcessingTime,
        errorRate: perfMetrics.errorRate,
        queueDepth: perfMetrics.queueDepth,
      },
      timestamp: Date.now(),
    };
  }

  private async getLastProcessedTime(): Promise<string> {
    try {
      const result = await this.dbPool.query(
        'SELECT MAX(timestamp) as last_processed FROM flashes WHERE ipfs_cid IS NOT NULL LIMIT 1'
      );
      return result.rows[0]?.last_processed?.toISOString() || 'never';
    } catch {
      return 'unknown';
    }
  }

  private getMemoryUsagePercent(): number {
    const memUsage = process.memoryUsage();
    return Math.round((memUsage.heapUsed / memUsage.heapTotal) * 100);
  }

  private getProcessingRate(): number {
    const metrics = metricsCollector.getPerformanceMetrics();
    return Math.round(metrics.processingRate * 100) / 100;
  }

  private getErrorRate(): number {
    const metrics = metricsCollector.getPerformanceMetrics();
    return Math.round(metrics.errorRate * 100) / 100;
  }
}
