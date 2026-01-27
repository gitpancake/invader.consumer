import { EventEmitter } from 'events';
import { RateLimiter } from '../rate-limiter';

export interface ConcurrentProcessorOptions {
  maxConcurrency?: number;
  rateLimiter?: RateLimiter;
  retryAttempts?: number;
  retryDelay?: number;
  onProgress?: (completed: number, total: number, active: number) => void;
  onError?: (error: Error, item: any) => void;
}

export interface ProcessingStats {
  total: number;
  completed: number;
  failed: number;
  active: number;
  avgProcessingTime: number;
  startTime: number;
  endTime?: number;
}

export class ConcurrentProcessor<T, R> extends EventEmitter {
  private active: number = 0;
  private completed: number = 0;
  private failed: number = 0;
  private processingTimes: number[] = [];
  private startTime: number = 0;

  constructor(private options: ConcurrentProcessorOptions = {}) {
    super();
    this.options = {
      maxConcurrency: 5,
      retryAttempts: 3,
      retryDelay: 1000,
      ...options
    };
  }

  /**
   * Process items concurrently with rate limiting and error handling
   */
  async processItems(
    items: T[],
    processor: (item: T) => Promise<R>
  ): Promise<{ results: R[]; stats: ProcessingStats }> {
    const results: R[] = [];
    const total = items.length;
    
    this.resetStats(total);
    
    console.log(`[ConcurrentProcessor] Starting processing of ${total} items (concurrency: ${this.options.maxConcurrency})`);

    return new Promise((resolve, reject) => {
      const itemQueue = [...items];
      const errors: Error[] = [];

      const processNext = async (): Promise<void> => {
        if (itemQueue.length === 0 && this.active === 0) {
          // All done
          const stats = this.getStats();
          console.log(`[ConcurrentProcessor] Processing complete: ${this.completed}/${total} successful, ${this.failed} failed`);
          resolve({ results, stats });
          return;
        }

        if (itemQueue.length === 0 || this.active >= this.options.maxConcurrency!) {
          return;
        }

        const item = itemQueue.shift()!;
        this.active++;

        try {
          // Apply rate limiting if configured
          if (this.options.rateLimiter) {
            await this.options.rateLimiter.waitIfNeeded();
          }

          const startTime = Date.now();
          const result = await this.processWithRetry(item, processor);
          const processingTime = Date.now() - startTime;
          
          this.processingTimes.push(processingTime);
          results.push(result);
          this.completed++;
          
          this.emit('itemCompleted', { item, result, processingTime });
          
        } catch (error) {
          this.failed++;
          const err = error as Error;
          errors.push(err);
          
          console.error(`[ConcurrentProcessor] Item processing failed:`, err.message);
          this.emit('itemFailed', { item, error: err });
          
          if (this.options.onError) {
            this.options.onError(err, item);
          }
        } finally {
          this.active--;
          
          // Progress callback
          if (this.options.onProgress) {
            this.options.onProgress(this.completed, total, this.active);
          }

          // Progress logging every 10%
          const progress = ((this.completed + this.failed) / total) * 100;
          if (progress > 0 && progress % 10 === 0) {
            const avgTime = this.getAverageProcessingTime();
            console.log(`[ConcurrentProcessor] Progress: ${this.completed + this.failed}/${total} (${progress.toFixed(1)}%) - Avg time: ${avgTime}ms`);
          }

          // Continue processing
          setImmediate(processNext);
        }
      };

      // Start concurrent processing
      for (let i = 0; i < Math.min(this.options.maxConcurrency!, itemQueue.length); i++) {
        processNext().catch(reject);
      }
    });
  }

  /**
   * Process item with retry logic
   */
  private async processWithRetry<T, R>(
    item: T,
    processor: (item: T) => Promise<R>,
    attempt: number = 1
  ): Promise<R> {
    try {
      return await processor(item);
    } catch (error) {
      if (attempt < this.options.retryAttempts!) {
        console.log(`[ConcurrentProcessor] Retry attempt ${attempt}/${this.options.retryAttempts} for item`);
        
        // Exponential backoff delay
        const delay = this.options.retryDelay! * Math.pow(2, attempt - 1);
        await this.delay(delay);
        
        return this.processWithRetry(item, processor, attempt + 1);
      }
      throw error;
    }
  }

  /**
   * Process items in controlled batches with lane management
   */
  async processInLanes(
    items: T[],
    processor: (item: T) => Promise<R>,
    laneCount: number = 3
  ): Promise<{ results: R[]; stats: ProcessingStats }> {
    const lanes: T[][] = [];
    
    // Distribute items across lanes
    for (let i = 0; i < items.length; i++) {
      const laneIndex = i % laneCount;
      if (!lanes[laneIndex]) {
        lanes[laneIndex] = [];
      }
      lanes[laneIndex].push(items[i]);
    }

    console.log(`[ConcurrentProcessor] Processing ${items.length} items across ${laneCount} lanes`);
    
    this.resetStats(items.length);
    
    // Process each lane concurrently
    const lanePromises = lanes.map(async (laneItems, laneIndex) => {
      console.log(`[ConcurrentProcessor] Lane ${laneIndex + 1}: ${laneItems.length} items`);
      
      const laneResults: R[] = [];
      for (const item of laneItems) {
        try {
          // Apply rate limiting
          if (this.options.rateLimiter) {
            await this.options.rateLimiter.waitIfNeeded();
          }

          const startTime = Date.now();
          const result = await this.processWithRetry(item, processor);
          const processingTime = Date.now() - startTime;
          
          this.processingTimes.push(processingTime);
          laneResults.push(result);
          this.completed++;
          
          // Progress update
          if (this.options.onProgress) {
            this.options.onProgress(this.completed, items.length, lanes.length);
          }
          
        } catch (error) {
          this.failed++;
          console.error(`[ConcurrentProcessor] Lane ${laneIndex + 1} item failed:`, error);
          
          if (this.options.onError) {
            this.options.onError(error as Error, item);
          }
        }
      }
      
      console.log(`[ConcurrentProcessor] Lane ${laneIndex + 1} completed: ${laneResults.length}/${laneItems.length} successful`);
      return laneResults;
    });

    const laneResults = await Promise.all(lanePromises);
    const results = laneResults.flat();
    const stats = this.getStats();
    
    console.log(`[ConcurrentProcessor] All lanes completed: ${results.length}/${items.length} successful`);
    
    return { results, stats };
  }

  /**
   * Reset processing statistics
   */
  private resetStats(total: number): void {
    this.active = 0;
    this.completed = 0;
    this.failed = 0;
    this.processingTimes = [];
    this.startTime = Date.now();
  }

  /**
   * Get current processing statistics
   */
  getStats(): ProcessingStats {
    return {
      total: this.completed + this.failed,
      completed: this.completed,
      failed: this.failed,
      active: this.active,
      avgProcessingTime: this.getAverageProcessingTime(),
      startTime: this.startTime,
      endTime: this.active === 0 ? Date.now() : undefined
    };
  }

  /**
   * Get average processing time
   */
  private getAverageProcessingTime(): number {
    if (this.processingTimes.length === 0) return 0;
    return Math.round(this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length);
  }

  /**
   * Utility delay function
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get estimated time remaining
   */
  getETA(): number | null {
    if (this.processingTimes.length === 0) return null;
    
    const avgTime = this.getAverageProcessingTime();
    const remaining = this.getStats().total - this.completed;
    
    return remaining * avgTime;
  }

  /**
   * Stop processing gracefully
   */
  stop(): void {
    this.removeAllListeners();
    console.log('[ConcurrentProcessor] Stopped processing');
  }
}

// Factory function for common use cases
export function createImageProcessor<T, R>(options: ConcurrentProcessorOptions = {}) {
  return new ConcurrentProcessor<T, R>({
    maxConcurrency: 3, // Conservative for image processing
    retryAttempts: 2,
    retryDelay: 2000,
    ...options
  });
}

export function createAPIProcessor(rateLimiter: RateLimiter, options: ConcurrentProcessorOptions = {}) {
  return new ConcurrentProcessor({
    maxConcurrency: 5,
    rateLimiter,
    retryAttempts: 3,
    retryDelay: 1000,
    ...options
  });
}