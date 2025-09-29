interface BatchUpdate {
  flash_id: number;
  ipfs_cid: string;
}

interface FailedUpdate extends BatchUpdate {
  reason: string;
}

interface FlashRecord {
  ipfs_cid: string | null;
}

export interface BatchUpdateResult {
  successful: number;
  failed: FailedUpdate[];
  alreadyProcessed: number[];
}

export class BatchUpdater {
  private batch: BatchUpdate[] = [];
  private dbPool: any;
  private batchSize: number;
  private flushTimer: NodeJS.Timeout | null = null;
  private static failureCount: number = 0;
  private static readonly FAILURE_LOG_THRESHOLD = 5;

  constructor(dbPool: any, batchSize: number = 300) {
    this.dbPool = dbPool;
    // Adjust batch size based on environment and available memory
    this.batchSize = process.env.NODE_ENV === 'production'
      ? Math.min(batchSize, 300) // Cap at 300 in production for memory efficiency
      : batchSize;
  }

  async addUpdate(flash_id: number, ipfs_cid: string): Promise<void> {
    // Check if this flash_id already has an IPFS CID to avoid race conditions
    try {
      const existingCheck = await this.dbPool.query("SELECT ipfs_cid FROM flashes WHERE flash_id = $1", [flash_id]);

      if (existingCheck.rows.length > 0 && existingCheck.rows[0].ipfs_cid) {
        // Already has IPFS CID, skip adding to batch
        return;
      }
    } catch (dbError) {
      console.error(`[BatchUpdater] Database error checking existing CID for flash ${flash_id}:`, dbError);
      // Continue adding to batch even if check fails - update will handle duplicates
    }

    this.batch.push({ flash_id, ipfs_cid });

    // Auto-flush if batch is full
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    } else {
      // Set a timer to flush after 5 seconds if batch isn't full
      this.resetFlushTimer();
    }
  }

  private resetFlushTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
    }

    this.flushTimer = setTimeout(() => {
      if (this.batch.length > 0) {
        this.flush().catch((err) => console.error("[BatchUpdater] Timer flush failed:", err));
      }
    }, 5000);
  }

  async flush(): Promise<BatchUpdateResult> {
    if (this.batch.length === 0) {
      return { successful: 0, failed: [], alreadyProcessed: [] };
    }

    const batchToProcess = [...this.batch];
    this.batch = []; // Clear the batch immediately

    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    const retryBatchUpdate = async (attempt: number = 1): Promise<any> => {
      try {
        // Only log batch operations in development or on retries
        if (process.env.NODE_ENV !== 'production' || attempt > 1) {
          console.log(`[${new Date().toISOString()}] [BatchUpdater] Writing ${batchToProcess.length} IPFS CIDs to database${attempt > 1 ? ` (attempt ${attempt})` : ''}...`);
        }

        // Use parameterized queries instead of string concatenation for memory efficiency
        const flashIds = batchToProcess.map(u => u.flash_id);
        const ipfsCids = batchToProcess.map(u => u.ipfs_cid);

        const query = `
          UPDATE flashes
          SET ipfs_cid = data.ipfs_cid
          FROM (
            SELECT UNNEST($1::int[]) AS flash_id, UNNEST($2::text[]) AS ipfs_cid
          ) AS data
          WHERE flashes.flash_id = data.flash_id
          AND (flashes.ipfs_cid IS NULL OR flashes.ipfs_cid = '')
        `;

        const result = await this.dbPool.query(query, [flashIds, ipfsCids]);
        // Log the number of rows affected for debugging
        if (process.env.NODE_ENV !== 'production' || result.rowCount === 0) {
          console.log(`[${new Date().toISOString()}] [BatchUpdater] UPDATE query affected ${result.rowCount} rows out of ${batchToProcess.length} attempted`);
        }
        return result;
      } catch (error) {
        const isConnectionError = error instanceof Error && 
          (error.message.includes('Connection terminated') || 
           error.message.includes('ECONNRESET') ||
           error.message.includes('connection timeout') ||
           error.message.includes('connection pool'));
        
        if (isConnectionError && attempt < 3) {
          console.log(`[BatchUpdater] Database connection error, retrying in ${attempt * 2}s (attempt ${attempt}/3)...`);
          await new Promise(resolve => setTimeout(resolve, attempt * 2000));
          return retryBatchUpdate(attempt + 1);
        }
        throw error;
      }
    };

    try {
      await retryBatchUpdate();

      // Process results in memory-efficient chunks to avoid loading all data at once
      const processResult = await this.processResultsInChunks(batchToProcess);
      const { successful, failed, alreadyProcessed } = processResult;

      // Log accurate results after verification
      if (successful > 0) {
        console.log(`[${new Date().toISOString()}] [BatchUpdater] ✅ Successfully verified ${successful} IPFS CID updates in database`);
      }

      if (failed.length > 0) {
        BatchUpdater.failureCount++;
        if (BatchUpdater.failureCount >= BatchUpdater.FAILURE_LOG_THRESHOLD) {
          // Group failures by reason for cleaner logging
          const failuresByReason = new Map<string, FailedUpdate[]>();
          failed.forEach(f => {
            if (!failuresByReason.has(f.reason)) {
              failuresByReason.set(f.reason, []);
            }
            failuresByReason.get(f.reason)!.push(f);
          });

          console.warn(`[${new Date().toISOString()}] [BatchUpdater] ⚠️ ${failed.length} database updates failed (${BatchUpdater.failureCount} consecutive failures):`);
          failuresByReason.forEach((failures, reason) => {
            console.warn(`  - ${reason}: ${failures.map(f => f.flash_id).join(', ')} (will be requeued)`);
          });
          BatchUpdater.failureCount = 0; // Reset counter after logging
        }
      } else {
        BatchUpdater.failureCount = 0; // Reset counter on success
      }

      if (alreadyProcessed.length > 0) {
        // Only log skipped records in development
        if (process.env.NODE_ENV !== 'production' && alreadyProcessed.length > 0) {
          console.log(`[BatchUpdater] ✅ ${alreadyProcessed.length} records skipped (already have IPFS CIDs)`);
        }
      }

      return {
        successful,
        failed,
        alreadyProcessed
      };

    } catch (error) {
      console.error(`[BatchUpdater] ❌ Batch update failed:`, error);

      // If batch completely failed, try individual updates and return detailed results
      const fallbackResult = await this.fallbackIndividualUpdates(batchToProcess);
      return fallbackResult;
    }
  }

  private async processResultsInChunks(batchToProcess: BatchUpdate[], chunkSize: number = 100): Promise<{ successful: number; failed: FailedUpdate[]; alreadyProcessed: number[] }> {
    const failed: FailedUpdate[] = [];
    const alreadyProcessed: number[] = [];
    let successful = 0;

    // Process results in chunks to reduce memory footprint
    for (let i = 0; i < batchToProcess.length; i += chunkSize) {
      const chunk = batchToProcess.slice(i, i + chunkSize);
      const chunkIds = chunk.map(u => u.flash_id);

      try {
        const checkResult = await this.dbPool.query(
          "SELECT flash_id, ipfs_cid FROM flashes WHERE flash_id = ANY($1::int[])",
          [chunkIds]
        );

        // Debug logging to see what the SELECT actually returns
        console.log(`[${new Date().toISOString()}] [BatchUpdater] Verification query searched for: ${chunkIds.join(', ')}`);
        console.log(`[${new Date().toISOString()}] [BatchUpdater] Verification query found: ${checkResult.rows.length} records - ${checkResult.rows.map((r: any) => `${r.flash_id}:${r.ipfs_cid || 'NULL'}`).join(', ')}`);

        const existingRecords = new Map<number, FlashRecord>(
          checkResult.rows.map((r: any) => [r.flash_id, { ipfs_cid: r.ipfs_cid }])
        );

        // Process this chunk
        for (const update of chunk) {
          const existingRecord = existingRecords.get(update.flash_id);

          if (!existingRecord) {
            // Record doesn't exist in DB - this is a failure case
            console.log(`[${new Date().toISOString()}] [BatchUpdater] DEBUG: flash_id ${update.flash_id} not found in verification results. Available flash_ids: ${checkResult.rows.map((r: any) => r.flash_id).join(', ')}`);
            failed.push({ ...update, reason: 'Record not found in database' });
          } else if (existingRecord.ipfs_cid && existingRecord.ipfs_cid !== '' && existingRecord.ipfs_cid !== update.ipfs_cid) {
            // Record already had a different IPFS CID - already processed, don't requeue
            alreadyProcessed.push(update.flash_id);
          } else if (existingRecord.ipfs_cid === update.ipfs_cid) {
            // Successfully updated with our CID
            successful++;
          } else if (!existingRecord.ipfs_cid || existingRecord.ipfs_cid === '') {
            // Should have been updated but wasn't - failure case (only if no ipfs_cid)
            failed.push({ ...update, reason: 'Update failed - IPFS CID still empty after update' });
          }
        }
      } catch (error) {
        console.error(`[BatchUpdater] Error processing chunk starting at index ${i}:`, error);
        // Mark entire chunk as failed with database error reason
        const errorMessage = error instanceof Error ? error.message : String(error);
        failed.push(...chunk.map(update => ({ ...update, reason: `Database error: ${errorMessage}` })));
      }
    }

    return { successful, failed, alreadyProcessed };
  }

  private async fallbackIndividualUpdates(batch: BatchUpdate[]): Promise<BatchUpdateResult> {
    let successful = 0;
    const failed: FailedUpdate[] = [];
    const alreadyProcessed: number[] = [];

    for (const update of batch) {
      try {
        const result = await this.dbPool.query(
          "UPDATE flashes SET ipfs_cid = $1 WHERE flash_id = $2 AND ipfs_cid IS NULL", 
          [update.ipfs_cid, update.flash_id]
        );
        
        if (result.rowCount > 0) {
          successful++;
        } else {
          // Check if it already has an IPFS CID
          const checkResult = await this.dbPool.query("SELECT ipfs_cid FROM flashes WHERE flash_id = $1", [update.flash_id]);
          if (checkResult.rows.length > 0 && checkResult.rows[0].ipfs_cid) {
            alreadyProcessed.push(update.flash_id);
          } else {
            // No record found or other issue
            failed.push({ ...update, reason: 'Record not found in database' });
          }
        }
      } catch (error) {
        console.error(`[BatchUpdater] Failed individual update for flash_id ${update.flash_id}:`, error);
        const errorMessage = error instanceof Error ? error.message : String(error);
        failed.push({ ...update, reason: `Database error: ${errorMessage}` });
      }
    }

    // Only log individual update summary in development
    if (process.env.NODE_ENV !== 'production') {
      console.log(`[BatchUpdater] Individual database updates complete: ${successful} successful, ${failed.length} failed, ${alreadyProcessed.length} already processed`);
    }
    
    return {
      successful,
      failed,
      alreadyProcessed
    };
  }

  async forceFlush(): Promise<BatchUpdateResult> {
    return await this.flush();
  }

  getBatchSize(): number {
    return this.batch.length;
  }

  async shutdown(): Promise<void> {
    // Process any remaining database updates before shutdown
    const result = await this.flush();
    if (result.failed.length > 0) {
      console.warn(`[BatchUpdater] Shutdown: ${result.failed.length} database updates failed during final write`);
    }
  }
}
