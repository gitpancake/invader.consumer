import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import axios from "axios";
import { config } from "dotenv";
import { Flash } from "../types/Flash";
import { getPool, closePool } from "../util/database";
import { CSVLogger, IPFSRecord } from "../util/csv-logger";
import { proxyRotator } from "../util/proxy";

config({ path: ".env" });

const PINATA_JWT = process.env.PINATA_JWT;
if (!PINATA_JWT) {
  throw new Error("PINATA_JWT environment variable is required");
}

// AWS S3 configuration (for dual storage)
const REGION = process.env.AWS_REGION;
const BUCKET_NAME = process.env.BUCKET_NAME;
const s3 = new S3Client({ region: REGION });

const BASE_URL = "https://api.space-invaders.com";

// Common user agents to rotate through for obfuscation
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

// Common referrers to make requests look more natural
const REFERRERS = [
  "https://www.google.com/",
  "https://www.bing.com/",
  "https://www.reddit.com/",
];

function getRandomUserAgent(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function getRandomReferrer(): string {
  return REFERRERS[Math.floor(Math.random() * REFERRERS.length)];
}

function getRandomDelay(): number {
  return Math.random() * 4000 + 1000;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getRealisticHeaders(): Record<string, string> {
  const userAgent = getRandomUserAgent();
  const referrer = getRandomReferrer();

  return {
    "User-Agent": userAgent,
    Accept: "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8,fr;q=0.7,de;q=0.6",
    "Accept-Encoding": "gzip, deflate, br",
    DNT: "1",
    Connection: "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "image",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Referer: referrer,
  };
}

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

      const delay = baseDelay * Math.pow(2, attempt) + Math.random() * 1000;
      console.log(`Request failed, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${maxRetries + 1})`);
      await sleep(delay);
    }
  }

  throw lastError!;
}

async function getRandomFlash(): Promise<Flash> {
  console.log("🔍 Fetching a random flash from database...");
  
  const pool = getPool();
  const result = await pool.query(`
    SELECT flash_id, img, city, text, player, timestamp, flash_count 
    FROM flashes 
    WHERE img IS NOT NULL 
    AND img != ''
    ORDER BY RANDOM() 
    LIMIT 1
  `);
  
  if (result.rows.length === 0) {
    throw new Error("No flashes found in database");
  }
  
  const row = result.rows[0];
  const flash: Flash = {
    flash_id: row.flash_id,
    img: row.img,
    city: row.city,
    text: row.text,
    player: row.player,
    timestamp: row.timestamp,
    flash_count: row.flash_count
  };
  
  console.log(`📸 Selected flash_id: ${flash.flash_id}, img: ${flash.img}`);
  return flash;
}

async function processFlash(flash: Flash): Promise<void> {
  console.log(`\\n🚀 Processing flash_id: ${flash.flash_id}`);
  
  const imageUrl = BASE_URL + flash.img;
  const originalKey = flash.img.replace(/^\//, "");

  try {
    // Add human-like delay before making the request
    const delay = getRandomDelay();
    console.log(`⏱️  Waiting ${Math.round(delay)}ms before requesting image...`);
    await sleep(delay);

    const response = await retryRequest(async () => {
      const headers = getRealisticHeaders();
      
      // Get proxy agent for this request
      const { agent, proxy } = proxyRotator.createProxyAgent(imageUrl);
      
      if (proxy) {
        console.log(`Using proxy: ${proxy.host}:${proxy.port} for ${flash.flash_id}`);
      }

      try {
        return await axios.get(imageUrl, {
          responseType: "arraybuffer",
          headers,
          timeout: 30000,
          maxRedirects: 5,
          validateStatus: (status) => status < 400,
          // Use proxy agent if available
          ...(imageUrl.startsWith('https://') ? { httpsAgent: agent } : { httpAgent: agent }),
        });
      } catch (error) {
        // Mark proxy as failed if this was a proxy request
        proxyRotator.handleProxyFailure(proxy);
        throw error;
      }
    });

    const contentType = response.headers["content-type"] || "image/jpeg";
    const contentLength = response.headers["content-length"];
    const fileSize = parseInt(contentLength || "0", 10) || response.data.byteLength;

    console.log(`📥 Successfully downloaded image (${fileSize} bytes, ${contentType})`);

    const filename = originalKey.split('/').pop() || `image_${flash.flash_id}.jpg`;

    let s3Success = false;
    let ipfsSuccess = false;
    let cid = null;

    // 1. Upload to S3
    try {
      console.log(`☁️  Uploading to S3...`);
      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: originalKey,
          Body: response.data,
          ContentType: contentType,
          ACL: "public-read",
          Metadata: {
            "original-url": imageUrl,
            "downloaded-at": new Date().toISOString(),
            "user-agent": String(response.config.headers?.["User-Agent"] || "unknown"),
            "test-upload": "true",
          },
        })
      );

      console.log(`✅ S3: https://${BUCKET_NAME}.s3.amazonaws.com/${originalKey}`);
      s3Success = true;
    } catch (s3Error) {
      console.error(`❌ S3 upload failed:`, s3Error instanceof Error ? s3Error.message : s3Error);
    }

    // 2. Upload to IPFS via Pinata
    try {
      console.log(`🌐 Uploading to IPFS via Pinata...`);
      const file = new File([response.data], filename, { type: contentType });
      const formData = new FormData();
      formData.append('file', file);
      formData.append('pinataMetadata', JSON.stringify({ 
        name: filename,
        keyvalues: {
          flash_id: flash.flash_id.toString(),
          test: "true"
        }
      }));

      const pinataResponse = await axios.post('https://api.pinata.cloud/pinning/pinFileToIPFS', formData, {
        headers: {
          'Authorization': `Bearer ${PINATA_JWT}`,
          'Content-Type': 'multipart/form-data'
        },
        timeout: 60000
      });

      cid = pinataResponse.data.IpfsHash;
      const ipfsUrl = `ipfs://${cid}`;
      
      console.log(`✅ IPFS: ${ipfsUrl}`);
      ipfsSuccess = true;
    } catch (ipfsError) {
      console.error(`❌ IPFS upload failed:`, ipfsError instanceof Error ? ipfsError.message : ipfsError);
    }

    // 3. Update database with IPFS CID if successful
    if (ipfsSuccess && cid) {
      console.log(`💾 Updating database with IPFS CID...`);
      const pool = getPool();
      await pool.query(
        `UPDATE flashes 
         SET ipfs_cid = $1 
         WHERE flash_id = $2`,
        [cid, flash.flash_id]
      );
      console.log(`✅ Database updated`);
    }

    // 4. Log to CSV for Web3.Storage import later (only if IPFS successful)
    if (ipfsSuccess && cid) {
      const csvRecord: IPFSRecord = {
        flash_id: flash.flash_id,
        cid,
        filename,
        ipfs_url: `ipfs://${cid}`,
        file_size: fileSize,
        content_type: contentType,
        uploaded_at: new Date().toISOString(),
        source: 'API'
      };
      
      CSVLogger.logIPFSUpload(csvRecord);
      console.log(`📝 Logged to CSV for Web3.Storage import`);
    }

    // 5. Report final status
    console.log(`\\n📊 FINAL STATUS:`);
    if (s3Success && ipfsSuccess) {
      console.log(`🎉 SUCCESS: Both S3 and IPFS uploads completed!`);
    } else if (s3Success) {
      console.log(`⚠️  PARTIAL: S3 succeeded, IPFS failed`);
    } else if (ipfsSuccess) {
      console.log(`⚠️  PARTIAL: IPFS succeeded, S3 failed`);
    } else {
      console.log(`💥 FAILURE: Both S3 and IPFS uploads failed`);
      throw new Error('Both S3 and IPFS uploads failed');
    }

    // Add a small delay after successful processing
    await sleep(Math.random() * 2000 + 500);
    
  } catch (err) {
    console.error("❌ Error processing flash:", err);
    throw err;
  }
}

async function runTest(): Promise<void> {
  console.log("🧪 Starting dual upload test...");
  console.log("=====================================");
  
  try {
    // Get a random flash from the database
    const flash = await getRandomFlash();
    
    // Process it using the same logic as the RabbitMQ consumer
    await processFlash(flash);
    
    console.log("\\n🎉 Test completed successfully!");
    
  } catch (error) {
    console.error("💥 Test failed:", error);
    process.exit(1);
  }
}

// Run the test if this file is executed directly
if (require.main === module) {
  runTest()
    .then(() => {
      console.log("Test script completed");
      return closePool();
    })
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      console.error("Test script failed:", error);
      closePool().finally(() => process.exit(1));
    });
}

export { runTest };