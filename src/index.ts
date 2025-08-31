import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ConsumeMessage } from "amqplib";
import axios from "axios";
import { config } from "dotenv";
import { Flash } from "./types/Flash";
import { BatchUpdater } from "./util/batch-updater";
import { CSVLogger, IPFSRecord } from "./util/csv-logger";
import { getPool } from "./util/database";
import { RabbitMQBaseConsumer } from "./util/rabbitmq";
import { RateLimiter } from "./util/rate-limiter";

config({
  path: ".env",
});

const PINATA_JWT = process.env.PINATA_JWT;
if (!PINATA_JWT) {
  throw new Error("PINATA_JWT environment variable is required");
}

// AWS S3 configuration (for dual storage)
const REGION = process.env.AWS_REGION;
const BUCKET_NAME = process.env.BUCKET_NAME;
const s3 = new S3Client({ region: REGION });
const BASE_URL = "https://api.space-invaders.com";

// Rate limiter for IPFS uploads - 50 req/min total during migration period
const ipfsRateLimiter = new RateLimiter(1200); // 1.2 seconds = 50 req/min

// Common user agents to rotate through for obfuscation
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
];

// Common referrers to make requests look more natural
const REFERRERS = [
  "https://www.google.com/",
  "https://www.bing.com/",
  "https://www.reddit.com/",
  "https://www.facebook.com/",
  "https://www.twitter.com/",
  "https://www.instagram.com/",
  "https://www.youtube.com/",
  "https://www.linkedin.com/",
  "https://www.github.com/",
  "https://www.stackoverflow.com/",
];

// Function to get a random user agent
function getRandomUserAgent(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

// Function to get a random referrer
function getRandomReferrer(): string {
  return REFERRERS[Math.floor(Math.random() * REFERRERS.length)];
}

// Function to add human-like delay
function getRandomDelay(): number {
  // Random delay between 1-5 seconds to mimic human browsing
  return Math.random() * 4000 + 1000;
}

// Function to sleep for a given number of milliseconds
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Function to get realistic browser headers
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
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "X-Requested-With": "XMLHttpRequest",
  };
}

// Function to retry failed requests with exponential backoff
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

      // Exponential backoff with jitter
      const delay = baseDelay * Math.pow(2, attempt) + Math.random() * 1000;
      console.log(`[FlashConsumer] Request failed, retrying in ${Math.round(delay)}ms (attempt ${attempt + 1}/${maxRetries + 1})`);
      await sleep(delay);
    }
  }

  throw lastError!;
}

class FlashConsumer extends RabbitMQBaseConsumer {
  private dbPool: any;
  public batchUpdater: BatchUpdater; // Make public for shutdown access

  constructor() {
    super();
    // Initialize database connection pool early
    this.dbPool = getPool();
    // Initialize batch updater for efficient database updates
    this.batchUpdater = new BatchUpdater(this.dbPool, 600);
  }

  protected async handleMessage(msg: ConsumeMessage): Promise<void> {
    const content = msg.content.toString();
    const flash: Flash = JSON.parse(content);
    const imageUrl = BASE_URL + flash.img;
    const originalKey = flash.img.replace(/^\//, "");

    try {
      // Add human-like delay before making the request
      const delay = getRandomDelay();
      // console.log(`[FlashConsumer] Waiting ${Math.round(delay)}ms before requesting image...`);
      await sleep(delay);

      const response = await retryRequest(async () => {
        const headers = getRealisticHeaders();
        // console.log(`[FlashConsumer] Requesting image with User-Agent: ${headers["User-Agent"].substring(0, 50)}...`);

        return await axios.get(imageUrl, {
          responseType: "arraybuffer",
          headers,
          timeout: 30000,
          maxRedirects: 5,
          validateStatus: (status) => status < 400, // Only accept 2xx and 3xx status codes
        });
      });

      const contentType = response.headers["content-type"] || "image/jpeg";
      const contentLength = response.headers["content-length"];
      const fileSize = parseInt(contentLength || "0", 10) || response.data.byteLength;

      const filename = originalKey.split("/").pop() || `image_${flash.flash_id}.jpg`;
      let s3Success = false;
      let ipfsSuccess = false;
      let cid = null;

      // 1. Upload to S3 (existing functionality)
      try {
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
            },
          })
        );

        s3Success = true;
      } catch (s3Error) {
        console.error(`[FlashConsumer] ❌ S3 upload failed:`, s3Error instanceof Error ? s3Error.message : s3Error);
      }

      // 2. Upload to IPFS via Pinata (with rate limiting and retry)
      try {
        const uploadToIPFS = async (): Promise<string> => {
          // Apply rate limiting for IPFS uploads
          await ipfsRateLimiter.waitIfNeeded();

          const file = new File([response.data], filename, { type: contentType });
          const formData = new FormData();
          formData.append("file", file);
          formData.append("pinataMetadata", JSON.stringify({ name: filename }));

          const pinataResponse = await axios.post("https://api.pinata.cloud/pinning/pinFileToIPFS", formData, {
            headers: {
              Authorization: `Bearer ${PINATA_JWT}`,
              "Content-Type": "multipart/form-data",
            },
            timeout: 60000,
            validateStatus: (status) => status < 500, // Retry on 5xx errors
          });

          // Handle rate limiting with longer wait
          if (pinataResponse.status === 429) {
            const retryAfter = pinataResponse.headers['retry-after'] || '60';
            const waitTime = parseInt(retryAfter) * 1000;
            throw new Error(`Rate limited by Pinata API - suggested wait: ${waitTime}ms`);
          }

          if (pinataResponse.status >= 400) {
            throw new Error(`Pinata API error: ${pinataResponse.status} ${pinataResponse.statusText}`);
          }

          return pinataResponse.data.IpfsHash;
        };

        // Retry IPFS upload with longer backoff during migration period
        cid = await retryRequest(uploadToIPFS, 5, 10000); // 5 retries, 10 second base delay for migration period
        ipfsSuccess = true;
      } catch (ipfsError) {
        const errorMsg = ipfsError instanceof Error ? ipfsError.message : "Unknown error";
        console.error(`[FlashConsumer] ❌ IPFS upload failed after retries: ${errorMsg}`);
      }

      // 3. Add to batch updater if IPFS was successful
      if (ipfsSuccess && cid) {
        try {
          await this.batchUpdater.addUpdate(flash.flash_id, cid);
        } catch (batchError) {
          console.error(`[FlashConsumer] ❌ Batch update failed:`, batchError instanceof Error ? batchError.message : batchError);
        }
      }

      // 4. Report status
      if (s3Success && ipfsSuccess) {
        console.log(`Dual upload complete for flash_id: ${flash.flash_id}, ipfs: ${cid}, s3: ✅`);
      } else if (s3Success) {
        console.log(`Dual upload failed for flash_id: ${flash.flash_id}, reason: IPFS failed`);
      } else if (ipfsSuccess) {
        console.log(`Dual upload failed for flash_id: ${flash.flash_id}, reason: S3 failed`);
      } else {
        throw new Error("Both S3 and IPFS uploads failed");
      }

      // Log to CSV for Web3.Storage import later (only if IPFS successful)
      if (ipfsSuccess && cid) {
        const csvRecord: IPFSRecord = {
          flash_id: flash.flash_id,
          cid,
          filename,
          ipfs_url: `ipfs://${cid}`,
          file_size: fileSize,
          content_type: contentType,
          uploaded_at: new Date().toISOString(),
          source: "API", // Consumer always downloads from API
        };

        CSVLogger.logIPFSUpload(csvRecord);
      }

      // Add small processing delay to be gentle on the system
      await sleep(300);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : "Unknown error";
      console.log(`Dual upload failed for flash_id: ${flash.flash_id}, reason: ${errorMsg}`);
      throw err;
    }
  }
}

(async () => {
  const consumer = new FlashConsumer();
  const testMode = process.env.TEST_MODE === "true";

  // Graceful shutdown handler
  const shutdown = async (signal: string) => {
    console.log(`\n[FlashConsumer] Received ${signal}, shutting down gracefully...`);

    try {
      // Force flush any remaining batch updates
      await consumer.batchUpdater.shutdown();
      console.log(`[FlashConsumer] Batch updater shutdown complete`);

      // Close database connections
      const { closePool } = await import("./util/database");
      await closePool();
      console.log(`[FlashConsumer] Database connections closed`);

      process.exit(0);
    } catch (error) {
      console.error(`[FlashConsumer] Error during shutdown:`, error);
      process.exit(1);
    }
  };

  // Register shutdown handlers
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  await consumer.startConsuming(testMode);
})();
