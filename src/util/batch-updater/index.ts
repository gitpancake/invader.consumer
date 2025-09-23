interface BatchUpdate {
  flash_id: number;
  ipfs_cid: string;
}

export interface BatchUpdateResult {
  successful: number;
  failed: BatchUpdate[];
  alreadyProcessed: number[];
}

export class BatchUpdater {
  private batch: BatchUpdate[] = [];
  private dbPool: any;
  private batchSize: number;
  private flushTimer: NodeJS.Timeout | null = null;

  constructor(dbPool: any, batchSize: number = 600) {
    this.dbPool = dbPool;
    this.batchSize = batchSize;
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
        console.log(`[BatchUpdater] Writing ${batchToProcess.length} IPFS CIDs to database${attempt > 1 ? ` (attempt ${attempt})` : ''}...`);

        // Build the bulk update query
        const values = batchToProcess.map((update) => `(${update.flash_id}, '${update.ipfs_cid}')`).join(",");

        const query = `
          UPDATE flashes 
          SET ipfs_cid = updates.ipfs_cid
          FROM (VALUES ${values}) AS updates(flash_id, ipfs_cid)
          WHERE flashes.flash_id = updates.flash_id
          AND flashes.ipfs_cid IS NULL
        `;

        const result = await this.dbPool.query(query);
        console.log(`[BatchUpdater] ✅ Successfully wrote IPFS CIDs to ${result.rowCount} database records`);
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
      const result = await retryBatchUpdate();

      // Determine which records were successful vs failed
      const allBatchIds = batchToProcess.map((u) => u.flash_id);
      const checkResult = await this.dbPool.query("SELECT flash_id, ipfs_cid FROM flashes WHERE flash_id = ANY($1::int[])", [allBatchIds]);

      const existingRecords = new Map(checkResult.rows.map((r: any) => [r.flash_id, r.ipfs_cid]));
      const failed: BatchUpdate[] = [];
      const alreadyProcessed: number[] = [];
      let successful = 0;

      for (const update of batchToProcess) {
        const existingRecord = existingRecords.get(update.flash_id);
        
        if (!existingRecord) {
          // Record doesn't exist in DB - this is a failure case
          failed.push(update);
        } else if (existingRecord.ipfs_cid && existingRecord.ipfs_cid !== update.ipfs_cid) {
          // Record already had a different IPFS CID - already processed
          alreadyProcessed.push(update.flash_id);
        } else if (existingRecord.ipfs_cid === update.ipfs_cid) {
          // Successfully updated with our CID
          successful++;
        } else if (!existingRecord.ipfs_cid) {
          // Should have been updated but wasn't - failure case
          failed.push(update);
        }
      }

      if (failed.length > 0) {
        console.warn(`[BatchUpdater] ⚠️ ${failed.length} database updates failed and will be requeued to RabbitMQ: ${failed.map(f => f.flash_id).join(', ')}`);
      }

      if (alreadyProcessed.length > 0) {
        console.log(`[BatchUpdater] ✅ ${alreadyProcessed.length} records skipped (already have IPFS CIDs)`);
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

  private async fallbackIndividualUpdates(batch: BatchUpdate[]): Promise<BatchUpdateResult> {
    let successful = 0;
    const failed: BatchUpdate[] = [];
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
            failed.push(update);
          }
        }
      } catch (error) {
        console.error(`[BatchUpdater] Failed individual update for flash_id ${update.flash_id}:`, error);
        failed.push(update);
      }
    }

    console.log(`[BatchUpdater] Individual database updates complete: ${successful} successful, ${failed.length} failed, ${alreadyProcessed.length} already processed`);
    
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
