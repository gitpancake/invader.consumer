import { Pool, PoolClient, PoolConfig } from 'pg';
import { EventEmitter } from 'events';
import { configManager } from '../config';
import { metricsCollector } from '../monitoring/metrics';

export interface PoolStats {
  totalCount: number;
  idleCount: number;
  waitingCount: number;
  connectionsCreated: number;
  connectionsDestroyed: number;
  queryCount: number;
  errorCount: number;
}

export class DatabaseConnectionPool extends EventEmitter {
  private pool: Pool;
  private stats: PoolStats = {
    totalCount: 0,
    idleCount: 0,
    waitingCount: 0,
    connectionsCreated: 0,
    connectionsDestroyed: 0,
    queryCount: 0,
    errorCount: 0
  };
  private monitoringInterval?: NodeJS.Timeout;

  constructor() {
    super();
    this.pool = this.createPool();
    this.setupEventListeners();
    this.startMonitoring();
  }

  /**
   * Create optimized database pool
   */
  private createPool(): Pool {
    const dbConfig = configManager.getDatabase();
    const isProduction = configManager.isProduction();

    const poolConfig: PoolConfig = {
      connectionString: process.env.DATABASE_URL,
      max: dbConfig.pool.max,
      min: dbConfig.pool.min,
      idleTimeoutMillis: dbConfig.pool.idle,
      connectionTimeoutMillis: dbConfig.pool.acquire,
      
      // Production optimizations
      ...(isProduction && {
        statement_timeout: 30000,
        query_timeout: 30000,
        application_name: 'invaders-consumer',
        keepAlive: true,
        keepAliveInitialDelayMillis: 10000
      })
    };

    console.log(`[DatabaseConnectionPool] Creating pool with config:`, {
      max: poolConfig.max,
      min: poolConfig.min,
      idleTimeout: poolConfig.idleTimeoutMillis,
      connectionTimeout: poolConfig.connectionTimeoutMillis
    });

    return new Pool(poolConfig);
  }

  /**
   * Setup pool event listeners for monitoring
   */
  private setupEventListeners(): void {
    this.pool.on('connect', (client: PoolClient) => {
      this.stats.connectionsCreated++;
      console.log(`[DatabaseConnectionPool] New connection established (total: ${this.pool.totalCount})`);
      this.emit('connect', client);
    });

    this.pool.on('remove', (client: PoolClient) => {
      this.stats.connectionsDestroyed++;
      console.log(`[DatabaseConnectionPool] Connection removed (total: ${this.pool.totalCount})`);
      this.emit('remove', client);
    });

    this.pool.on('error', (err: Error, client: PoolClient) => {
      this.stats.errorCount++;
      console.error('[DatabaseConnectionPool] Pool error:', err);
      this.emit('error', err, client);
      
      // Record error metrics
      metricsCollector.incrementCounter('database.pool.errors');
    });

    this.pool.on('acquire', (client: PoolClient) => {
      this.emit('acquire', client);
    });

    // Note: 'release' event doesn't exist in pg Pool, only 'connect', 'acquire', 'remove', 'error'
    // this.pool.on('release', (client: PoolClient) => {
    //   this.emit('release', client);
    // });
  }

  /**
   * Start monitoring pool statistics
   */
  private startMonitoring(): void {
    const monitoringConfig = configManager.getMonitoring();
    
    if (monitoringConfig.enabled) {
      this.monitoringInterval = setInterval(() => {
        this.updateStats();
        this.recordMetrics();
      }, monitoringConfig.interval);
      
      console.log('[DatabaseConnectionPool] Pool monitoring started');
    }
  }

  /**
   * Update pool statistics
   */
  private updateStats(): void {
    this.stats.totalCount = this.pool.totalCount;
    this.stats.idleCount = this.pool.idleCount;
    this.stats.waitingCount = this.pool.waitingCount;
  }

  /**
   * Record pool metrics
   */
  private recordMetrics(): void {
    metricsCollector.recordMetric('database.pool.total', this.stats.totalCount);
    metricsCollector.recordMetric('database.pool.idle', this.stats.idleCount);
    metricsCollector.recordMetric('database.pool.waiting', this.stats.waitingCount);
    metricsCollector.recordMetric('database.pool.queries', this.stats.queryCount);
    metricsCollector.recordMetric('database.pool.errors', this.stats.errorCount);

    // Calculate pool utilization
    const utilization = this.stats.totalCount > 0 
      ? ((this.stats.totalCount - this.stats.idleCount) / this.stats.totalCount) * 100 
      : 0;
    metricsCollector.recordMetric('database.pool.utilization', utilization);
  }

  /**
   * Execute a query with automatic retries and metrics
   */
  async query(text: string, params?: any[]): Promise<any> {
    const startTime = Date.now();
    const dbConfig = configManager.getDatabase();
    
    let lastError: Error | null = null;
    
    for (let attempt = 1; attempt <= dbConfig.retries.max + 1; attempt++) {
      try {
        const result = await this.pool.query(text, params);
        
        // Record success metrics
        this.stats.queryCount++;
        const queryTime = Date.now() - startTime;
        metricsCollector.recordMetric('database.query.duration', queryTime);
        metricsCollector.incrementCounter('database.queries.success');
        
        // Log slow queries in development
        if (configManager.isDevelopment() && queryTime > 5000) {
          console.warn(`[DatabaseConnectionPool] Slow query (${queryTime}ms):`, text.substring(0, 100));
        }
        
        return result;
        
      } catch (error) {
        lastError = error as Error;
        this.stats.errorCount++;
        
        const isRetryableError = this.isRetryableError(lastError);
        
        if (attempt <= dbConfig.retries.max && isRetryableError) {
          const delay = dbConfig.retries.delay * Math.pow(2, attempt - 1);
          console.warn(`[DatabaseConnectionPool] Query failed, retrying in ${delay}ms (attempt ${attempt}/${dbConfig.retries.max + 1}):`, lastError.message);
          
          await this.delay(delay);
          continue;
        }
        
        // Record failure metrics
        metricsCollector.incrementCounter('database.queries.error');
        metricsCollector.recordMetric('database.query.duration', Date.now() - startTime);
        
        console.error(`[DatabaseConnectionPool] Query failed after ${attempt} attempts:`, lastError.message);
        throw lastError;
      }
    }
    
    throw lastError;
  }

  /**
   * Get a client from the pool with timeout
   */
  async getClient(): Promise<PoolClient> {
    const startTime = Date.now();
    
    try {
      const client = await this.pool.connect();
      const waitTime = Date.now() - startTime;
      
      if (waitTime > 5000) {
        console.warn(`[DatabaseConnectionPool] Client acquisition took ${waitTime}ms`);
      }
      
      metricsCollector.recordMetric('database.client.acquisition_time', waitTime);
      return client;
      
    } catch (error) {
      console.error('[DatabaseConnectionPool] Failed to acquire client:', error);
      metricsCollector.incrementCounter('database.client.acquisition_errors');
      throw error;
    }
  }

  /**
   * Execute a transaction with automatic retry
   */
  async transaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.getClient();
    const startTime = Date.now();
    
    try {
      await client.query('BEGIN');
      
      const result = await callback(client);
      
      await client.query('COMMIT');
      
      const transactionTime = Date.now() - startTime;
      metricsCollector.recordMetric('database.transaction.duration', transactionTime);
      metricsCollector.incrementCounter('database.transactions.success');
      
      return result;
      
    } catch (error) {
      await client.query('ROLLBACK');
      
      const transactionTime = Date.now() - startTime;
      metricsCollector.recordMetric('database.transaction.duration', transactionTime);
      metricsCollector.incrementCounter('database.transactions.error');
      
      console.error('[DatabaseConnectionPool] Transaction failed:', error);
      throw error;
      
    } finally {
      client.release();
    }
  }

  /**
   * Check if error is retryable
   */
  private isRetryableError(error: Error): boolean {
    const retryableErrors = [
      'connection terminated',
      'connection timeout',
      'connection refused',
      'connection reset',
      'server closed the connection unexpectedly',
      'connection to server was lost',
      'connection pool exhausted',
      'timeout expired'
    ];

    const message = error.message.toLowerCase();
    return retryableErrors.some(retryableError => message.includes(retryableError));
  }

  /**
   * Get current pool statistics
   */
  getStats(): PoolStats {
    this.updateStats();
    return { ...this.stats };
  }

  /**
   * Health check for pool
   */
  async healthCheck(): Promise<{ healthy: boolean; details: PoolStats & { responseTime: number } }> {
    const startTime = Date.now();
    
    try {
      await this.query('SELECT 1');
      const responseTime = Date.now() - startTime;
      const stats = this.getStats();
      
      const healthy = responseTime < 5000 && stats.waitingCount < 10;
      
      return {
        healthy,
        details: {
          ...stats,
          responseTime
        }
      };
      
    } catch (error) {
      return {
        healthy: false,
        details: {
          ...this.getStats(),
          responseTime: Date.now() - startTime
        }
      };
    }
  }

  /**
   * Gracefully shutdown the pool
   */
  async shutdown(): Promise<void> {
    if (this.monitoringInterval) {
      clearInterval(this.monitoringInterval);
    }
    
    console.log('[DatabaseConnectionPool] Shutting down connection pool...');
    
    try {
      await this.pool.end();
      console.log('[DatabaseConnectionPool] Pool shutdown completed');
    } catch (error) {
      console.error('[DatabaseConnectionPool] Error during shutdown:', error);
    }
    
    this.removeAllListeners();
  }

  /**
   * Get the underlying pool (for compatibility)
   */
  getPool(): Pool {
    return this.pool;
  }

  /**
   * Utility delay function
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Export singleton instance
export const databasePool = new DatabaseConnectionPool();