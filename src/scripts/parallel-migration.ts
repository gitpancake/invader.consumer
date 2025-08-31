#!/usr/bin/env node

import { config } from "dotenv";
import { spawn, ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { getPool } from '../util/database';

config({ path: ".env" });

interface WorkerConfig {
  id: number;
  batchSize: number;
  maxBatches: number;
  offset: number;
  targetImages: number;
}

interface WorkerStatus {
  id: number;
  pid?: number;
  status: 'starting' | 'running' | 'completed' | 'failed' | 'stopped';
  processed: number;
  migrated: number;
  failed: number;
  currentBatch: number;
  startTime: Date;
  lastUpdate: Date;
  logFile: string;
}

class ParallelMigrationManager {
  private workers: Map<number, ChildProcess> = new Map();
  private workerStatus: Map<number, WorkerStatus> = new Map();
  private logDir: string;
  private isShuttingDown = false;
  private totalRecordsToMigrate = 0;

  constructor() {
    this.logDir = path.join(process.cwd(), 'migration-logs');
    this.ensureLogDir();
    this.setupSignalHandlers();
  }

  private ensureLogDir() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  private setupSignalHandlers() {
    process.on('SIGINT', () => this.gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => this.gracefulShutdown('SIGTERM'));
  }

  private async gracefulShutdown(signal: string) {
    if (this.isShuttingDown) return;
    
    console.log(`\n🛑 Received ${signal}. Shutting down workers gracefully...`);
    this.isShuttingDown = true;
    
    // Stop all workers
    for (const [workerId, worker] of this.workers) {
      if (worker && !worker.killed) {
        console.log(`   Stopping worker ${workerId}...`);
        worker.kill('SIGTERM');
      }
    }
    
    // Wait up to 10 seconds for graceful shutdown
    setTimeout(() => {
      console.log('🔴 Force killing remaining workers...');
      for (const [workerId, worker] of this.workers) {
        if (worker && !worker.killed) {
          worker.kill('SIGKILL');
        }
      }
      process.exit(1);
    }, 10000);
  }

  private getWorkerConfigs(): WorkerConfig[] {
    const totalImages = this.totalRecordsToMigrate;
    const numWorkers = 6;
    const imagesPerWorker = Math.ceil(totalImages / numWorkers);
    const batchSize = 50;
    
    return Array.from({ length: numWorkers }, (_, i) => ({
      id: i + 1,
      batchSize,
      maxBatches: Math.ceil(imagesPerWorker / batchSize),
      offset: i * imagesPerWorker,
      targetImages: Math.min(imagesPerWorker, totalImages - (i * imagesPerWorker))
    }));
  }

  private initializeWorkerStatus(config: WorkerConfig): WorkerStatus {
    const logFile = path.join(this.logDir, `worker-${config.id}.log`);
    return {
      id: config.id,
      status: 'starting',
      processed: 0,
      migrated: 0,
      failed: 0,
      currentBatch: 0,
      startTime: new Date(),
      lastUpdate: new Date(),
      logFile
    };
  }

  private parseWorkerOutput(workerId: number, data: string) {
    const status = this.workerStatus.get(workerId);
    if (!status) return;

    // Update last seen time
    status.lastUpdate = new Date();

    // Parse migration progress from output
    const lines = data.toString().split('\n');
    for (const line of lines) {
      // Look for batch progress: "Starting migration: X per batch, Y batches max"
      if (line.includes('Starting migration:')) {
        status.status = 'running';
      }
      
      // Look for progress updates: "Worker X Progress - Batch Y: Processed Z, Migrated A, Failed B"
      const progressMatch = line.match(/Worker \d+ Progress - Batch (\d+): Processed (\d+), Migrated (\d+), Failed (\d+)/);
      if (progressMatch) {
        status.currentBatch = parseInt(progressMatch[1]);
        status.processed = parseInt(progressMatch[2]);
        status.migrated = parseInt(progressMatch[3]);
        status.failed = parseInt(progressMatch[4]);
      }
      
      // Look for completion stats: "Total processed: X"
      const totalMatch = line.match(/Total processed: (\d+)/);
      if (totalMatch) {
        status.processed = parseInt(totalMatch[1]);
      }
      
      // Look for success stats: "Successfully migrated: X"  
      const migratedMatch = line.match(/Successfully migrated: (\d+)/);
      if (migratedMatch) {
        status.migrated = parseInt(migratedMatch[1]);
      }
      
      // Look for failure stats: "Failed: X"
      const failedMatch = line.match(/Failed: (\d+)/);
      if (failedMatch) {
        status.failed = parseInt(failedMatch[1]);
      }
      
      // Look for individual success/failure: "flash_id: cid" or "❌ flash_id: error"
      const successMatch = line.match(/^(\d+): Qm[a-zA-Z0-9]+/);
      if (successMatch && status.status === 'running') {
        status.processed++;
        status.migrated++;
      }
      
      const errorMatch = line.match(/^❌ (\d+):/);
      if (errorMatch && status.status === 'running') {
        status.processed++;
        status.failed++;
      }
      
      // Look for completion message
      if (line.includes('MIGRATION COMPLETE')) {
        status.status = 'completed';
      }
      
      // Estimate current batch from processed count if not explicitly set
      if (status.processed > 0 && !progressMatch) {
        status.currentBatch = Math.ceil(status.processed / 50);
      }
    }
  }

  private startWorker(config: WorkerConfig): ChildProcess {
    const status = this.initializeWorkerStatus(config);
    this.workerStatus.set(config.id, status);

    // Build the migration command
    const args = [
      'dist/scripts/migrate-s3-to-ipfs.js',
      config.batchSize.toString(),
      config.maxBatches.toString()
    ];

    console.log(`🚀 Starting worker ${config.id}: ${config.targetImages.toLocaleString()} images (${config.maxBatches} batches of 50)`);

    const worker = spawn('node', args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { 
        ...process.env,
        WORKER_ID: config.id.toString(),
        WORKER_OFFSET: config.offset.toString()
      }
    });

    status.pid = worker.pid;

    // Create log file stream
    const logStream = fs.createWriteStream(status.logFile, { flags: 'w' });
    
    // Handle worker output
    worker.stdout?.on('data', (data) => {
      const output = data.toString();
      logStream.write(`[STDOUT ${new Date().toISOString()}] ${output}`);
      this.parseWorkerOutput(config.id, output);
    });

    worker.stderr?.on('data', (data) => {
      const output = data.toString();
      logStream.write(`[STDERR ${new Date().toISOString()}] ${output}`);
      // Also parse stderr for error stats
      this.parseWorkerOutput(config.id, output);
    });

    worker.on('close', (code) => {
      logStream.end();
      const status = this.workerStatus.get(config.id);
      if (status) {
        status.status = code === 0 ? 'completed' : 'failed';
        console.log(`\n${code === 0 ? '✅' : '❌'} Worker ${config.id} finished with code ${code}`);
      }
    });

    worker.on('error', (error) => {
      const status = this.workerStatus.get(config.id);
      if (status) {
        status.status = 'failed';
      }
      console.error(`❌ Worker ${config.id} error:`, error.message);
    });

    return worker;
  }

  private displayStatus() {
    if (this.isShuttingDown) return;

    console.clear();
    console.log('🔄 PARALLEL MIGRATION STATUS\n');
    console.log('=' .repeat(120));
    
    let totalProcessed = 0;
    let totalMigrated = 0;
    let totalFailed = 0;
    
    for (const [workerId, status] of this.workerStatus) {
      const runtime = Math.floor((Date.now() - status.startTime.getTime()) / 1000);
      const rate = status.processed > 0 ? (status.processed / runtime * 60).toFixed(1) : '0.0';
      const progress = status.processed > 0 ? ((status.migrated / status.processed) * 100).toFixed(1) : '0.0';
      
      const statusIcon = {
        'starting': '⏳',
        'running': '🔄',
        'completed': '✅', 
        'failed': '❌',
        'stopped': '⏹️'
      }[status.status] || '❓';
      
      console.log(`${statusIcon} Worker ${workerId.toString().padStart(2)}: ` +
        `${status.status.padEnd(9)} | ` +
        `Processed: ${status.processed.toString().padStart(6)} | ` +
        `Migrated: ${status.migrated.toString().padStart(6)} | ` + 
        `Failed: ${status.failed.toString().padStart(4)} | ` +
        `Success: ${progress}% | ` +
        `Rate: ${rate}/min | ` +
        `Runtime: ${Math.floor(runtime/60)}:${(runtime%60).toString().padStart(2,'0')}`
      );
      
      totalProcessed += status.processed;
      totalMigrated += status.migrated;
      totalFailed += status.failed;
    }
    
    console.log('=' .repeat(120));
    console.log(`📊 TOTALS: Processed: ${totalProcessed.toLocaleString()} | ` +
      `Migrated: ${totalMigrated.toLocaleString()} | ` +
      `Failed: ${totalFailed.toLocaleString()} | ` +
      `Remaining: ${(this.totalRecordsToMigrate - totalProcessed).toLocaleString()}`);
    
    const overallProgress = this.totalRecordsToMigrate > 0 ? (totalProcessed / this.totalRecordsToMigrate * 100).toFixed(2) : '0.00';
    console.log(`🎯 Overall Progress: ${overallProgress}% complete`);
    
    console.log(`\n📝 Log files: ${this.logDir}`);
    console.log('Press Ctrl+C to gracefully stop all workers\n');
  }

  async start() {
    // Get actual count from database
    console.log('📊 Fetching count of records to migrate...');
    const pool = getPool();
    const countResult = await pool.query(`
      SELECT COUNT(*) as count
      FROM flashes 
      WHERE ipfs_cid IS NULL
      AND img IS NOT NULL
      AND img != ''
    `);
    this.totalRecordsToMigrate = parseInt(countResult.rows[0].count);
    
    console.log(`🎯 Starting Parallel Migration for ${this.totalRecordsToMigrate.toLocaleString()} images...\n`);
    
    // Ensure the project is built
    console.log('📦 Building TypeScript...');
    const buildProcess = spawn('yarn', ['build'], { stdio: 'inherit' });
    
    await new Promise((resolve, reject) => {
      buildProcess.on('close', (code) => {
        if (code === 0) {
          resolve(void 0);
        } else {
          reject(new Error(`Build failed with code ${code}`));
        }
      });
    });
    
    const configs = this.getWorkerConfigs();
    
    // Start all workers
    for (const config of configs) {
      const worker = this.startWorker(config);
      this.workers.set(config.id, worker);
      
      // Small delay between worker starts
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Start status display
    const statusInterval = setInterval(() => {
      this.displayStatus();
    }, 5000);
    
    // Initial display
    setTimeout(() => this.displayStatus(), 2000);
    
    // Wait for all workers to complete
    const promises = Array.from(this.workers.values()).map(worker => 
      new Promise<void>((resolve) => {
        worker.on('close', () => resolve());
      })
    );
    
    await Promise.all(promises);
    
    clearInterval(statusInterval);
    this.displayStatus(); // Final status
    
    console.log('\n🎉 All workers completed!');
  }
}

// Run if called directly
if (require.main === module) {
  const manager = new ParallelMigrationManager();
  manager.start().catch(error => {
    console.error('Migration failed:', error);
    process.exit(1);
  });
}

export { ParallelMigrationManager };