import { config } from "dotenv";

// Load environment variables FIRST before any other imports
config({ path: ".env" });

import { S3Client, GetObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
// Using Pinata HTTP API directly
import axios from "axios";
import { getPool, closePool } from "../util/database";
import { CSVLogger, IPFSRecord } from "../util/csv-logger";
import { proxyRotator } from "../util/proxy";

// AWS S3 configuration
const REGION = process.env.AWS_REGION;
const BUCKET_NAME = process.env.BUCKET_NAME;
const s3 = new S3Client({ region: REGION });

// Pinata configuration
const PINATA_JWT = process.env.PINATA_JWT;
if (!PINATA_JWT) {
  throw new Error("PINATA_JWT environment variable is required");
}

interface FlashRecord {
  flash_id: number;
  img: string;
}

interface MigrationStats {
  total: number;
  migrated: number;
  skipped: number;
  failed: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getFlashesToMigrate(limit: number = 100, offset: number = 0): Promise<FlashRecord[]> {
  const pool = getPool();
  
  // Support worker-based offset for parallel processing
  const workerOffset = parseInt(process.env.WORKER_OFFSET || "0");
  const totalOffset = offset + workerOffset;
  
  // Get total count
  const countResult = await pool.query(`
    SELECT COUNT(*) as count
    FROM flashes 
    WHERE ipfs_cid IS NULL
    AND img IS NOT NULL
    AND img != ''
    AND timestamp < NOW() - INTERVAL '7 days'
  `);
  
  const result = await pool.query(`
    SELECT flash_id, img 
    FROM flashes 
    WHERE ipfs_cid IS NULL
    AND img IS NOT NULL
    AND img != ''
    AND timestamp < NOW() - INTERVAL '7 days'
    ORDER BY timestamp ASC
    LIMIT $1 OFFSET $2
  `, [limit, totalOffset]);
  
  
  return result.rows;
}

async function migrateImageToIPFS(flash: FlashRecord, retryCount = 0): Promise<boolean> {
  try {
    const s3Key = flash.img.replace(/^\//, "");
    const s3Url = `https://invader-flashes.s3.amazonaws.com/${s3Key}`;
    
    console.log(`[Migration] Processing flash_id: ${flash.flash_id}, downloading from: ${s3Url}`);
    
    // Try downloading from S3 first, fallback to API if not found
    let response;
    let source = "S3";
    
    try {
      response = await axios.get(s3Url, {
        responseType: "arraybuffer",
        timeout: 30000,
        validateStatus: (status) => status < 400,
      });
      console.log(`[Migration] Downloaded from S3: ${s3Url}`);
    } catch (s3Error) {
      // If S3 fails, try the original API with proxy rotation
      const apiUrl = `https://api.space-invaders.com${flash.img}`;
      console.log(`[Migration] S3 failed, trying API: ${apiUrl}`);
      source = "API";
      
      const { agent, proxy } = proxyRotator.createProxyAgent(apiUrl);
      
      if (proxy) {
        console.log(`[Migration] Using proxy: ${proxy.host}:${proxy.port} for flash_id: ${flash.flash_id}`);
      }

      try {
        response = await axios.get(apiUrl, {
          responseType: "arraybuffer",
          timeout: 30000,
          validateStatus: (status) => status < 400,
          // Use proxy agent if available
          httpsAgent: agent,
        });
        console.log(`[Migration] Downloaded from API: ${apiUrl}`);
      } catch (apiError) {
        // Mark proxy as failed if this was a proxy request
        proxyRotator.handleProxyFailure(proxy, apiError as Error);
        throw apiError;
      }
    }
    
    const contentType = response.headers["content-type"] || "image/jpeg";
    const contentLength = response.headers["content-length"];
    const fileSize = parseInt(contentLength || "0", 10) || response.data.byteLength;
    
    // Create File for upload
    const filename = s3Key.split('/').pop() || `image_${flash.flash_id}.jpg`;
    const file = new File([response.data], filename, { type: contentType });
    
    // Upload to IPFS via Pinata
    const formData = new FormData();
    formData.append('file', file);
    formData.append('pinataMetadata', JSON.stringify({ name: filename }));

    const pinataResponse = await axios.post('https://api.pinata.cloud/pinning/pinFileToIPFS', formData, {
      headers: {
        'Authorization': `Bearer ${PINATA_JWT}`,
        'Content-Type': 'multipart/form-data'
      },
      timeout: 60000,
      validateStatus: (status) => status < 500 // Don't throw on 4xx errors, we'll handle them
    });

    // Handle rate limiting with exponential backoff
    if (pinataResponse.status === 429) {
      const maxRetries = 3;
      if (retryCount < maxRetries) {
        const backoffTime = Math.pow(2, retryCount) * 1000; // 1s, 2s, 4s
        console.log(`⚠️  ${flash.flash_id}: Rate limited, retrying in ${backoffTime}ms (attempt ${retryCount + 1}/${maxRetries})`);
        await sleep(backoffTime);
        return await migrateImageToIPFS(flash, retryCount + 1);
      } else {
        console.error(`❌ ${flash.flash_id}: Rate limit exceeded after ${maxRetries} retries`);
        return false;
      }
    }

    // Handle other HTTP errors
    if (pinataResponse.status >= 400) {
      console.error(`❌ ${flash.flash_id}: Pinata error ${pinataResponse.status}: ${pinataResponse.statusText}`);
      return false;
    }

    const cid = pinataResponse.data.IpfsHash;
    const ipfsUrl = `ipfs://${cid}`;
    
    // Update flashes table with IPFS CID (safe UPDATE operation)
    const pool = getPool();
    const result = await pool.query(
      `UPDATE flashes 
       SET ipfs_cid = $1 
       WHERE flash_id = $2 
       AND ipfs_cid IS NULL`,  // Only update if not already set (extra safety)
      [cid, flash.flash_id]
    );
    
    if (result.rowCount === 0) {
      return false;
    }
    
    console.log(`${flash.flash_id}: ${cid}`);

    // Log to CSV for Web3.Storage import later
    const csvRecord: IPFSRecord = {
      flash_id: flash.flash_id,
      cid,
      filename,
      ipfs_url: ipfsUrl,
      file_size: fileSize,
      content_type: contentType,
      uploaded_at: new Date().toISOString(),
      source: source as 'S3' | 'API'
    };
    
    CSVLogger.logIPFSUpload(csvRecord);
    
    // Delay to respect migration rate limit (50 req/min for migration)
    const migrationRateLimit = parseInt(process.env.MIGRATION_RATE_LIMIT || '50');
    const delayMs = Math.floor(60000 / migrationRateLimit);
    await sleep(delayMs);
    
    return true;
    
  } catch (error) {
    console.error(`❌ ${flash.flash_id}: ${error instanceof Error ? error.message : 'Unknown error'}`);
    return false;
  }
}

async function runMigration(batchSize: number = 50, maxBatches: number = -1) {
  const workerId = process.env.WORKER_ID || "1";
  const workerOffset = parseInt(process.env.WORKER_OFFSET || "0");
  
  // Display proxy status at startup
  if (proxyRotator.hasProxies()) {
    console.log(`🔗 [Migration] Proxy enabled: ${proxyRotator.getProxyCount()} proxies configured`);
  } else {
    console.log(`🌐 [Migration] No proxy configured - using direct connections`);
  }
  
  // Get and display total count of records without ipfs_cid at startup
  const pool = getPool();
  const countResult = await pool.query(`
    SELECT COUNT(*) as count
    FROM flashes 
    WHERE ipfs_cid IS NULL
    AND img IS NOT NULL
    AND img != ''
    AND timestamp < NOW() - INTERVAL '7 days'
  `);
  const totalRecordsToMigrate = parseInt(countResult.rows[0].count);
  
  console.log(`Starting migration: ${batchSize} per batch, ${maxBatches === -1 ? 'unlimited' : maxBatches} batches max`);
  console.log(`Worker ${workerId}: Starting from offset ${workerOffset}`);
  console.log(`📊 Total records without IPFS CID: ${totalRecordsToMigrate}`);
  
  const stats: MigrationStats = {
    total: 0,
    migrated: 0,
    skipped: 0,
    failed: 0
  };
  
  let batchCount = 0;
  let currentOffset = 0;
  
  // Parallel processing configuration
  const concurrency = parseInt(process.env.MIGRATION_CONCURRENCY || '5');
  console.log(`📊 Migration concurrency: ${concurrency} parallel workers`);
  
  while (maxBatches === -1 || batchCount < maxBatches) {
    const flashesToMigrate = await getFlashesToMigrate(batchSize, currentOffset);
    
    if (flashesToMigrate.length === 0) {
      break;
    }
    
    // Process batch in parallel with limited concurrency
    const chunks = [];
    for (let i = 0; i < flashesToMigrate.length; i += concurrency) {
      chunks.push(flashesToMigrate.slice(i, i + concurrency));
    }
    
    for (const chunk of chunks) {
      const promises = chunk.map(async (flash) => {
        stats.total++;
        const success = await migrateImageToIPFS(flash);
        if (success) {
          stats.migrated++;
        } else {
          stats.failed++;
        }
        return success;
      });
      
      // Wait for all parallel operations in this chunk to complete
      await Promise.all(promises);
    }
    
    batchCount++;
    currentOffset += batchSize;
    
    // Progress update every 10 batches
    if (batchCount % 10 === 0) {
      console.log(`Worker ${workerId} Progress - Batch ${batchCount}: Processed ${stats.total}, Migrated ${stats.migrated}, Failed ${stats.failed}`);
    }
    
    // Delay between batches to avoid rate limiting
    if (maxBatches === -1 || batchCount < maxBatches) {
      await sleep(2000);
    }
  }
  
  console.log(`\n=== MIGRATION COMPLETE ===`);
  console.log(`Total processed: ${stats.total}`);
  console.log(`Successfully migrated: ${stats.migrated}`);
  console.log(`Failed: ${stats.failed}`);
  console.log(`Success rate: ${stats.total > 0 ? ((stats.migrated / stats.total) * 100).toFixed(1) : 0}%`);
}

// CLI interface
const args = process.argv.slice(2);
const batchSize = args[0] ? parseInt(args[0]) : 50;
const maxBatches = args[1] ? parseInt(args[1]) : -1;

if (require.main === module) {
  runMigration(batchSize, maxBatches)
    .then(() => {
      console.log(`Migration completed successfully`);
      return closePool();
    })
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      console.error(`Migration failed:`, error);
      closePool().finally(() => process.exit(1));
    });
}

export { runMigration };