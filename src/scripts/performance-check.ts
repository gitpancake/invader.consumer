#!/usr/bin/env node

import { databasePool } from '../util/database/connection-pool';
import { metricsCollector } from '../util/monitoring/metrics';
import { memoryOptimizer } from '../util/memory/optimizer';
import { configManager } from '../util/config';

interface PerformanceReport {
  timestamp: string;
  environment: string;
  uptime: number;
  database: {
    poolStats: any;
    connectionTime: number;
    queryPerformance: any;
  };
  memory: {
    usage: any;
    optimization: any;
  };
  processing: {
    metrics: any;
    rates: any;
  };
  configuration: any;
}

async function performanceCheck(): Promise<void> {
  console.log('🔍 Starting performance check...\n');

  try {
    const startTime = Date.now();
    const report: PerformanceReport = {
      timestamp: new Date().toISOString(),
      environment: configManager.getEnvironment(),
      uptime: Math.floor(process.uptime()),
      database: await checkDatabasePerformance(),
      memory: await checkMemoryPerformance(),
      processing: await checkProcessingPerformance(),
      configuration: configManager.getSummary()
    };

    const totalTime = Date.now() - startTime;

    // Display results
    console.log('📊 PERFORMANCE REPORT');
    console.log('=' .repeat(50));
    console.log(`Environment: ${report.environment}`);
    console.log(`Timestamp: ${report.timestamp}`);
    console.log(`Uptime: ${formatUptime(report.uptime)}`);
    console.log(`Check Duration: ${totalTime}ms\n`);

    console.log('💾 DATABASE PERFORMANCE');
    console.log('-' .repeat(30));
    console.log(`Connection Time: ${report.database.connectionTime}ms`);
    console.log(`Pool Status: ${report.database.poolStats.totalCount} total, ${report.database.poolStats.idleCount} idle, ${report.database.poolStats.waitingCount} waiting`);
    console.log(`Pool Utilization: ${((report.database.poolStats.totalCount - report.database.poolStats.idleCount) / Math.max(report.database.poolStats.totalCount, 1) * 100).toFixed(1)}%`);
    console.log();

    console.log('🧠 MEMORY PERFORMANCE');
    console.log('-' .repeat(30));
    console.log(`Heap Used: ${report.memory.usage.heapUsed}MB`);
    console.log(`Heap Total: ${report.memory.usage.heapTotal}MB`);
    console.log(`Memory Usage: ${(report.memory.usage.usage * 100).toFixed(1)}%`);
    console.log(`RSS: ${report.memory.usage.rss}MB`);
    console.log(`External: ${report.memory.usage.external}MB`);
    console.log();

    console.log('⚡ PROCESSING PERFORMANCE');
    console.log('-' .repeat(30));
    console.log(`Processing Rate: ${report.processing.metrics.processingRate.toFixed(2)}/sec`);
    console.log(`Avg Processing Time: ${report.processing.metrics.avgProcessingTime.toFixed(0)}ms`);
    console.log(`Error Rate: ${report.processing.metrics.errorRate.toFixed(1)}%`);
    console.log(`Queue Depth: ${report.processing.metrics.queueDepth}`);
    console.log();

    console.log('⚙️  CONFIGURATION');
    console.log('-' .repeat(30));
    console.log(`Concurrency: ${report.configuration.concurrency}`);
    console.log(`Batch Size: ${report.configuration.batchSize}`);
    console.log(`Rate Limit: ${report.configuration.rateLimit}/min`);
    console.log(`DB Pool Max: ${report.configuration.dbPoolMax}`);
    console.log(`Monitoring: ${report.configuration.monitoringEnabled ? 'Enabled' : 'Disabled'}`);
    console.log(`Proxy: ${report.configuration.proxyEnabled ? 'Enabled' : 'Disabled'}`);
    console.log();

    // Performance recommendations
    console.log('💡 RECOMMENDATIONS');
    console.log('-' .repeat(30));
    await generateRecommendations(report);

    // Save report if in production
    if (configManager.isProduction()) {
      await saveReport(report);
    }

  } catch (error) {
    console.error('❌ Performance check failed:', error);
    process.exit(1);
  }
}

async function checkDatabasePerformance(): Promise<PerformanceReport['database']> {
  const startTime = Date.now();
  
  try {
    // Test database connectivity and get pool stats
    await databasePool.query('SELECT 1');
    const connectionTime = Date.now() - startTime;
    const poolStats = databasePool.getStats();
    
    // Test query performance with a more realistic query
    const queryStart = Date.now();
    await databasePool.query('SELECT COUNT(*) FROM flashes WHERE ipfs_cid IS NULL');
    const queryTime = Date.now() - queryStart;

    return {
      poolStats,
      connectionTime,
      queryPerformance: {
        simpleQuery: connectionTime,
        countQuery: queryTime
      }
    };
  } catch (error) {
    console.error('Database performance check failed:', error);
    return {
      poolStats: { error: (error as Error).message },
      connectionTime: Date.now() - startTime,
      queryPerformance: { error: (error as Error).message }
    };
  }
}

async function checkMemoryPerformance(): Promise<PerformanceReport['memory']> {
  const memStats = memoryOptimizer.getMemoryStats();
  
  return {
    usage: memStats,
    optimization: {
      gcAvailable: !!global.gc,
      gcThreshold: '85%'
    }
  };
}

async function checkProcessingPerformance(): Promise<PerformanceReport['processing']> {
  const metrics = metricsCollector.getPerformanceMetrics();
  
  // Get recent processing rates
  const imageStats = metricsCollector.getMetricStats('image.processing_time', 5 * 60 * 1000); // Last 5 minutes
  
  return {
    metrics,
    rates: {
      recentImageProcessing: imageStats
    }
  };
}

async function generateRecommendations(report: PerformanceReport): Promise<void> {
  const recommendations: string[] = [];

  // Memory recommendations
  if (report.memory.usage.usage > 0.8) {
    recommendations.push('⚠️  High memory usage detected - consider reducing batch size or increasing GC frequency');
  }

  // Database recommendations
  if (report.database.connectionTime > 5000) {
    recommendations.push('⚠️  Slow database connection - check database health and network latency');
  }

  if (report.database.poolStats.waitingCount > 5) {
    recommendations.push('⚠️  Database pool contention - consider increasing pool size');
  }

  // Processing recommendations
  if (report.processing.metrics.errorRate > 5) {
    recommendations.push('⚠️  High error rate detected - investigate error patterns and consider circuit breakers');
  }

  if (report.processing.metrics.avgProcessingTime > 30000) {
    recommendations.push('⚠️  Slow processing detected - consider optimizing image processing or reducing concurrency');
  }

  // Configuration recommendations
  if (report.configuration.concurrency > 5 && report.memory.usage.usage > 0.7) {
    recommendations.push('💡 Consider reducing concurrency to manage memory usage');
  }

  if (!report.configuration.monitoringEnabled) {
    recommendations.push('💡 Enable monitoring for better operational visibility');
  }

  if (recommendations.length === 0) {
    console.log('✅ All performance metrics are within normal ranges');
  } else {
    recommendations.forEach(rec => console.log(rec));
  }
}

async function saveReport(report: PerformanceReport): Promise<void> {
  try {
    const fs = require('fs').promises;
    const path = require('path');
    
    const reportsDir = path.join(process.cwd(), 'performance-reports');
    
    // Ensure reports directory exists
    try {
      await fs.mkdir(reportsDir, { recursive: true });
    } catch (error) {
      // Directory might already exist
    }
    
    const filename = `performance-${report.timestamp.split('T')[0]}.json`;
    const filepath = path.join(reportsDir, filename);
    
    await fs.writeFile(filepath, JSON.stringify(report, null, 2));
    console.log(`📁 Report saved: ${filepath}`);
  } catch (error) {
    console.error('Failed to save performance report:', error);
  }
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  if (days > 0) {
    return `${days}d ${hours}h ${minutes}m ${secs}s`;
  } else if (hours > 0) {
    return `${hours}h ${minutes}m ${secs}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  } else {
    return `${secs}s`;
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n👋 Performance check interrupted');
  await cleanup();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n👋 Performance check terminated');
  await cleanup();
  process.exit(0);
});

async function cleanup(): Promise<void> {
  try {
    await databasePool.shutdown();
    memoryOptimizer.cleanup();
    metricsCollector.shutdown();
  } catch (error) {
    console.error('Cleanup error:', error);
  }
}

// Run the performance check
if (require.main === module) {
  performanceCheck()
    .then(() => {
      console.log('\n✅ Performance check completed');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n❌ Performance check failed:', error);
      process.exit(1);
    });
}