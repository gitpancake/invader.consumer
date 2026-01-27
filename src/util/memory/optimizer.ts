import { EventEmitter } from 'events';

export interface MemoryStats {
  heapUsed: number;
  heapTotal: number;
  external: number;
  rss: number;
  usage: number;
}

export interface BatchProcessingOptions {
  batchSize?: number;
  maxConcurrency?: number;
  gcThreshold?: number;
  onProgress?: (processed: number, total: number) => void;
  onMemoryWarning?: (stats: MemoryStats) => void;
}

export class MemoryOptimizer extends EventEmitter {
  private gcThreshold: number = 0.85; // Trigger GC at 85% memory usage
  private isMonitoring: boolean = false;
  private monitoringInterval?: NodeJS.Timeout;

  constructor() {
    super();
  }

  /**
   * Get current memory statistics
   */
  getMemoryStats(): MemoryStats {
    const memUsage = process.memoryUsage();
    const usage = memUsage.heapUsed / memUsage.heapTotal;
    
    return {
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024), // MB
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024), // MB
      external: Math.round(memUsage.external / 1024 / 1024), // MB
      rss: Math.round(memUsage.rss / 1024 / 1024), // MB
      usage
    };
  }

  /**
   * Start memory monitoring with automatic GC
   */
  startMonitoring(intervalMs: number = 30000): void {
    if (this.isMonitoring) return;
    
    this.isMonitoring = true;
    this.monitoringInterval = setInterval(() => {
      const stats = this.getMemoryStats();
      
      if (stats.usage > this.gcThreshold) {
        console.log(`[MemoryOptimizer] Memory usage at ${(stats.usage * 100).toFixed(1)}% - triggering GC`);
        this.forceGarbageCollection();
        this.emit('memoryWarning', stats);
      }
    }, intervalMs);
    
    console.log(`[MemoryOptimizer] Memory monitoring started (threshold: ${(this.gcThreshold * 100)}%)`);
  }

  /**
   * Stop memory monitoring
   */
  stopMonitoring(): void {
    if (this.monitoringInterval) {
      clearInterval(this.monitoringInterval);
      this.monitoringInterval = undefined;
    }
    this.isMonitoring = false;
    console.log('[MemoryOptimizer] Memory monitoring stopped');
  }

  /**
   * Process items in memory-efficient batches with automatic GC
   */
  async processBatches<T, R>(
    items: T[],
    processor: (batch: T[]) => Promise<R[]>,
    options: BatchProcessingOptions = {}
  ): Promise<R[]> {
    const {
      batchSize = 50,
      maxConcurrency = 3,
      gcThreshold = this.gcThreshold,
      onProgress,
      onMemoryWarning
    } = options;

    const results: R[] = [];
    const total = items.length;
    let processed = 0;

    console.log(`[MemoryOptimizer] Processing ${total} items in batches of ${batchSize} (concurrency: ${maxConcurrency})`);

    // Process in chunks to manage memory
    for (let i = 0; i < items.length; i += batchSize * maxConcurrency) {
      const chunk = items.slice(i, i + (batchSize * maxConcurrency));
      
      // Split chunk into concurrent batches
      const batches: T[][] = [];
      for (let j = 0; j < chunk.length; j += batchSize) {
        batches.push(chunk.slice(j, j + batchSize));
      }

      // Process batches concurrently
      const batchPromises = batches.map(async (batch) => {
        try {
          return await processor(batch);
        } catch (error) {
          console.error(`[MemoryOptimizer] Batch processing error:`, error);
          return [];
        }
      });

      const batchResults = await Promise.all(batchPromises);
      
      // Flatten results and add to total
      for (const batchResult of batchResults) {
        results.push(...batchResult);
      }

      processed += chunk.length;
      
      // Progress callback
      if (onProgress) {
        onProgress(processed, total);
      }

      // Memory check and cleanup
      const stats = this.getMemoryStats();
      if (stats.usage > gcThreshold) {
        console.log(`[MemoryOptimizer] Memory usage at ${(stats.usage * 100).toFixed(1)}% - cleaning up`);
        
        if (onMemoryWarning) {
          onMemoryWarning(stats);
        }
        
        // Force garbage collection
        this.forceGarbageCollection();
        
        // Small delay to allow memory cleanup
        await this.delay(1000);
      }

      // Log progress every 10%
      const progress = (processed / total) * 100;
      if (progress % 10 === 0 || processed === total) {
        console.log(`[MemoryOptimizer] Progress: ${processed}/${total} (${progress.toFixed(1)}%) - Memory: ${stats.heapUsed}MB`);
      }
    }

    console.log(`[MemoryOptimizer] Batch processing complete: ${results.length} results`);
    return results;
  }

  /**
   * Process large streams with memory management
   */
  async processStream<T>(
    stream: AsyncIterable<T>,
    processor: (item: T) => Promise<void>,
    options: { batchSize?: number; gcInterval?: number } = {}
  ): Promise<void> {
    const { batchSize = 100, gcInterval = 500 } = options;
    
    let processed = 0;
    let batch: T[] = [];

    for await (const item of stream) {
      batch.push(item);
      
      if (batch.length >= batchSize) {
        await Promise.all(batch.map(processor));
        batch = [];
        processed += batchSize;
        
        // Periodic memory check and GC
        if (processed % gcInterval === 0) {
          const stats = this.getMemoryStats();
          if (stats.usage > this.gcThreshold) {
            this.forceGarbageCollection();
            await this.delay(100);
          }
        }
      }
    }

    // Process remaining items
    if (batch.length > 0) {
      await Promise.all(batch.map(processor));
      processed += batch.length;
    }

    console.log(`[MemoryOptimizer] Stream processing complete: ${processed} items processed`);
  }

  /**
   * Force garbage collection if available
   */
  forceGarbageCollection(): void {
    if (global.gc) {
      global.gc();
      const stats = this.getMemoryStats();
      console.log(`[MemoryOptimizer] GC completed - Memory: ${stats.heapUsed}MB (${(stats.usage * 100).toFixed(1)}%)`);
    } else {
      console.log('[MemoryOptimizer] GC not available - start with --expose-gc flag');
    }
  }

  /**
   * Clean up references and trigger GC
   */
  cleanup(): void {
    this.stopMonitoring();
    this.removeAllListeners();
    this.forceGarbageCollection();
  }

  /**
   * Utility delay function
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Set GC threshold (0-1)
   */
  setGcThreshold(threshold: number): void {
    if (threshold < 0 || threshold > 1) {
      throw new Error('GC threshold must be between 0 and 1');
    }
    this.gcThreshold = threshold;
    console.log(`[MemoryOptimizer] GC threshold set to ${(threshold * 100)}%`);
  }
}

// Export singleton instance
export const memoryOptimizer = new MemoryOptimizer();