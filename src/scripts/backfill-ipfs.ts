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

// Retry function with special handling for OxLabs 407 errors
async function retryRequest<T>(requestFn: () => Promise<T>, maxRetries: number = 3, baseDelay: number = 1000): Promise<T> {
  let lastError: Error;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await requestFn();
    } catch (error) {
      lastError = error as Error;

      if (attempt === maxRetries) {
        throw lastError;
      }

      // Check for OxLabs 407 errors - use shorter delay since they're often temporary
      const is407Error = lastError.message?.includes('407') || lastError.message?.includes('Proxy Authentication');
      const isOxLabsError = lastError.message?.includes('oxylabs');
      
      let delay: number;
      if (is407Error && isOxLabsError) {
        // Shorter delay for OxLabs 407 errors since they're often just temporary hiccups
        delay = 2000 + Math.random() * 1000; // 2-3 seconds
        // Silent retry for OxLabs 407 errors - no logging
      } else {
        // Exponential backoff for other errors
        delay = baseDelay * Math.pow(2, attempt) + Math.random() * 1000;
        console.log(`[Backfill] Request failed: ${lastError.message}, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${maxRetries + 1})`);
      }
      
      await sleep(delay);
    }
  }

  throw lastError!;
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

    // Download image from API with retry logic
    const response = await retryRequest(async () => {
      try {
        const result = await axios.get(imageUrl, {
          responseType: "arraybuffer",
          timeout: 30000,
          validateStatus: (status) => status < 400,
          // Use proxy agent if available
          httpsAgent: agent,
        });
        console.log(`[Backfill] Downloaded from API: ${imageUrl}`);
        return result;
      } catch (downloadError) {
        // Mark proxy as failed if this was a proxy request
        proxyRotator.handleProxyFailure(proxy, downloadError as Error);
        throw downloadError;
      }
    }, 5, 2000); // 5 retries with 2 second base delay for OxLabs issues
    
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
    
    // Delay to respect rate limits - 250 req/min = ~240ms between requests
    await sleep(250); // 250ms = ~240 req/min
    
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
  console.log(`Rate limit: ~15000 records/hour (250 req/min)`);
  
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