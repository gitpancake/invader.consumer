interface BatchUpdate {
  flash_id: number;
  ipfs_cid: string;
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
        this.flush().catch(err => 
          console.error('[BatchUpdater] Timer flush failed:', err)
        );
      }
    }, 5000);
  }
  
  async flush(): Promise<void> {
    if (this.batch.length === 0) return;
    
    const batchToProcess = [...this.batch];
    this.batch = []; // Clear the batch immediately
    
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    
    try {
      console.log(`[BatchUpdater] Flushing ${batchToProcess.length} IPFS CID updates...`);
      
      // Build the bulk update query
      const values = batchToProcess.map(update => `(${update.flash_id}, '${update.ipfs_cid}')`).join(',');
      
      const query = `
        UPDATE flashes 
        SET ipfs_cid = updates.ipfs_cid
        FROM (VALUES ${values}) AS updates(flash_id, ipfs_cid)
        WHERE flashes.flash_id = updates.flash_id::integer
      `;
      
      const result = await this.dbPool.query(query);
      console.log(`[BatchUpdater] ✅ Updated ${result.rowCount} records successfully`);
      
      // Log any records that weren't updated (flash_id didn't exist)
      if (result.rowCount < batchToProcess.length) {
        const notUpdated = batchToProcess.length - result.rowCount;
        console.warn(`[BatchUpdater] ⚠️  ${notUpdated} records were not found in database`);
      }
      
    } catch (error) {
      console.error(`[BatchUpdater] ❌ Batch update failed:`, error);
      
      // Fallback to individual updates for this batch
      console.log(`[BatchUpdater] 🔄 Falling back to individual updates...`);
      await this.fallbackIndividualUpdates(batchToProcess);
    }
  }
  
  private async fallbackIndividualUpdates(batch: BatchUpdate[]): Promise<void> {
    let successful = 0;
    let failed = 0;
    
    for (const update of batch) {
      try {
        await this.dbPool.query(
          'UPDATE flashes SET ipfs_cid = $1 WHERE flash_id = $2',
          [update.ipfs_cid, update.flash_id]
        );
        successful++;
      } catch (error) {
        console.error(`[BatchUpdater] Failed individual update for flash_id ${update.flash_id}:`, error);
        failed++;
      }
    }
    
    console.log(`[BatchUpdater] Fallback complete: ${successful} successful, ${failed} failed`);
  }
  
  async forceFlush(): Promise<void> {
    await this.flush();
  }
  
  getBatchSize(): number {
    return this.batch.length;
  }
  
  async shutdown(): Promise<void> {
    // Flush any remaining updates
    await this.flush();
  }
}