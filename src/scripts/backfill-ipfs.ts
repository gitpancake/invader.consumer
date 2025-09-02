import { config } from "dotenv";

// Load environment variables FIRST before any other imports
config({ path: ".env" });

import axios from "axios";
import { getPool, closePool } from "../util/database";
import { CSVLogger, IPFSRecord } from "../util/csv-logger";
import { proxyRotator } from "../util/proxy";

const PINATA_JWT = process.env.PINATA_JWT;
if (!PINATA_JWT) {
  throw new Error("PINATA_JWT environment variable is required");
}

const BASE_URL = "https://api.space-invaders.com";

interface FlashRecord {
  flash_id: number;
  img: string;
}

interface BackfillStats {
  total: number;
  processed: number;
  successful: number;
  failed: number;
  alreadyHasIPFS: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getRecordsToBackfill(limit: number = 1000, offset: number = 0): Promise<FlashRecord[]> {
  const pool = getPool();
  
  const result = await pool.query(`
    SELECT flash_id, img 
    FROM flashes 
    WHERE img IS NOT NULL 
    AND img != ''
    ORDER BY flash_id DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset]);
  
  return result.rows;
}

async function getRecordsWithoutIPFS(limit: number = 1000, offset: number = 0): Promise<FlashRecord[]> {
  const pool = getPool();
  
  const result = await pool.query(`
    SELECT flash_id, img 
    FROM flashes 
    WHERE img IS NOT NULL 
    AND img != ''
    AND ipfs_cid IS NULL
    ORDER BY flash_id DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset]);
  
  return result.rows;
}

async function hasIPFSCID(flashId: number): Promise<boolean> {
  const pool = getPool();
  
  const result = await pool.query(`
    SELECT ipfs_cid FROM flashes WHERE flash_id = $1
  `, [flashId]);
  
  return result.rows.length > 0 && result.rows[0].ipfs_cid !== null;
}

async function backfillFlashToIPFS(flash: FlashRecord, retryCount = 0): Promise<boolean> {
  try {
    // Check if this record already has IPFS CID
    const hasIPFS = await hasIPFSCID(flash.flash_id);
    if (hasIPFS) {
      console.log(`⏭️  ${flash.flash_id}: Already has IPFS CID, skipping`);
      return true; // Count as success but skip processing
    }

    const imageUrl = BASE_URL + flash.img;
    console.log(`[Backfill] Processing flash_id: ${flash.flash_id}, downloading from: ${imageUrl}`);
    
    // Get proxy agent for this request
    const { agent, proxy } = proxyRotator.createProxyAgent(imageUrl);
    
    if (proxy) {
      console.log(`[Backfill] Using proxy: ${proxy.host}:${proxy.port} for flash_id: ${flash.flash_id}`);
    }

    // Download image from API
    let response;
    try {
      response = await axios.get(imageUrl, {
        responseType: "arraybuffer",
        timeout: 30000,
        validateStatus: (status) => status < 400,
        // Use proxy agent if available
        httpsAgent: agent,
      });
      console.log(`[Backfill] Downloaded from API: ${imageUrl}`);
    } catch (downloadError) {
      // Mark proxy as failed if this was a proxy request
      proxyRotator.handleProxyFailure(proxy, downloadError as Error);
      throw downloadError;
    }
    
    const contentType = response.headers["content-type"] || "image/jpeg";
    const contentLength = response.headers["content-length"];
    const fileSize = parseInt(contentLength || "0", 10) || response.data.byteLength;
    
    // Create File for IPFS upload
    const originalKey = flash.img.replace(/^\//, "");
    const filename = originalKey.split('/').pop() || `image_${flash.flash_id}.jpg`;
    const file = new File([response.data], filename, { type: contentType });
    
    // Upload to IPFS via Pinata
    const formData = new FormData();
    formData.append('file', file);
    formData.append('pinataMetadata', JSON.stringify({ 
      name: filename,
      keyvalues: {
        flash_id: flash.flash_id.toString(),
        backfill: "true"
      }
    }));

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
        const backoffTime = Math.pow(2, retryCount) * 2000; // 2s, 4s, 8s
        console.log(`⚠️  ${flash.flash_id}: Rate limited, retrying in ${backoffTime}ms (attempt ${retryCount + 1}/${maxRetries})`);
        await sleep(backoffTime);
        return await backfillFlashToIPFS(flash, retryCount + 1);
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
    
    // Update flashes table with IPFS CID
    const pool = getPool();
    const result = await pool.query(
      `UPDATE flashes 
       SET ipfs_cid = $1 
       WHERE flash_id = $2 
       AND ipfs_cid IS NULL`,  // Only update if not already set (safety check)
      [cid, flash.flash_id]
    );
    
    if (result.rowCount === 0) {
      console.log(`⏭️  ${flash.flash_id}: Already has IPFS CID (race condition), skipping database update`);
      return true; // Still count as success
    }
    
    console.log(`✅ ${flash.flash_id}: ${cid}`);

    // Log to CSV for Web3.Storage import later
    const csvRecord: IPFSRecord = {
      flash_id: flash.flash_id,
      cid,
      filename,
      ipfs_url: ipfsUrl,
      file_size: fileSize,
      content_type: contentType,
      uploaded_at: new Date().toISOString(),
      source: 'API'
    };
    
    CSVLogger.logIPFSUpload(csvRecord);
    
    // Delay to respect rate limits - conservative for backfill
    await sleep(2000); // 2 seconds between requests
    
    return true;
    
  } catch (error) {
    console.error(`❌ ${flash.flash_id}: ${error instanceof Error ? error.message : 'Unknown error'}`);
    return false;
  }
}

async function runBackfill(recordCount: number = 1000, onlyMissing: boolean = true, batchSize: number = 50) {
  console.log(`🔄 Starting IPFS backfill for ${recordCount} records (onlyMissing: ${onlyMissing})...`);
  console.log(`📊 Batch size: ${batchSize}`);
  
  // Initialize proxy health checks BEFORE starting processing
  await proxyRotator.initialize();
  
  // Display proxy status after initialization
  if (proxyRotator.getTotalProxyCount() > 0) {
    const workingCount = proxyRotator.getProxyCount();
    console.log(`🔗 [Backfill] Proxy system enabled: ${workingCount}/${proxyRotator.getTotalProxyCount()} proxies verified working`);
    
    if (workingCount === 0) {
      console.log(`🚨 [Backfill] WARNING: No working proxies found! All proxies failed health checks.`);
      console.log(`🌐 [Backfill] Proceeding with direct connections - may encounter rate limiting`);
    }
  } else {
    console.log(`🌐 [Backfill] No proxy configured - using direct connections`);
  }
  
  const stats: BackfillStats = {
    total: 0,
    processed: 0,
    successful: 0,
    failed: 0,
    alreadyHasIPFS: 0
  };
  
  let currentOffset = 0;
  
  while (stats.processed < recordCount) {
    const remainingRecords = recordCount - stats.processed;
    const currentBatchSize = Math.min(batchSize, remainingRecords);
    
    // Get records to process
    const records = onlyMissing 
      ? await getRecordsWithoutIPFS(currentBatchSize, currentOffset)
      : await getRecordsToBackfill(currentBatchSize, currentOffset);
    
    if (records.length === 0) {
      console.log('No more records to process');
      break;
    }
    
    console.log(`\n📦 Processing batch: ${stats.processed + 1}-${stats.processed + records.length} of ${recordCount}`);
    
    for (const record of records) {
      stats.total++;
      stats.processed++;
      
      // Check if already has IPFS before processing
      const hasIPFS = await hasIPFSCID(record.flash_id);
      if (hasIPFS) {
        stats.alreadyHasIPFS++;
        console.log(`⏭️  ${record.flash_id}: Already has IPFS CID, skipping`);
        continue;
      }
      
      const success = await backfillFlashToIPFS(record);
      
      if (success) {
        stats.successful++;
      } else {
        stats.failed++;
      }
      
      // Progress update every 25 records
      if (stats.processed % 25 === 0) {
        console.log(`📊 Progress: ${stats.processed}/${recordCount} | Success: ${stats.successful} | Failed: ${stats.failed} | Already has IPFS: ${stats.alreadyHasIPFS}`);
      }
    }
    
    if (!onlyMissing) {
      currentOffset += records.length;
    }
  }
  
  console.log(`\n=== BACKFILL COMPLETE ===`);
  console.log(`Total records checked: ${stats.total}`);
  console.log(`Successfully backfilled: ${stats.successful}`);
  console.log(`Failed: ${stats.failed}`);
  console.log(`Already had IPFS: ${stats.alreadyHasIPFS}`);
  console.log(`Success rate: ${stats.total > 0 ? ((stats.successful / (stats.total - stats.alreadyHasIPFS)) * 100).toFixed(1) : 0}%`);
}

// CLI interface
const args = process.argv.slice(2);
const recordCount = args[0] ? parseInt(args[0]) : 1000;
const onlyMissing = args[1] !== 'all'; // Default to only missing records unless 'all' is specified
const batchSize = args[2] ? parseInt(args[2]) : 50;

if (require.main === module) {
  console.log(`🎯 IPFS Backfill Tool`);
  console.log(`Records to process: ${recordCount}`);
  console.log(`Mode: ${onlyMissing ? 'Only missing IPFS records' : 'All records (latest first)'}`);
  console.log(`Batch size: ${batchSize}`);
  console.log(`Rate limit: ~1800 records/hour (2s delay between uploads)`);
  
  runBackfill(recordCount, onlyMissing, batchSize)
    .then(() => {
      console.log(`Backfill completed successfully`);
      return closePool();
    })
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      console.error(`Backfill failed:`, error);
      closePool().finally(() => process.exit(1));
    });
}

export { runBackfill };