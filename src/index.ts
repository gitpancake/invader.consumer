import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ConsumeMessage } from "amqplib";
import axios from "axios";
import { config } from "dotenv";
import { Flash } from "./types/Flash";
import { RabbitMQBaseConsumer } from "./util/rabbitmq";

config({
  path: ".env",
});

const REGION = process.env.AWS_REGION;
const BUCKET_NAME = process.env.BUCKET_NAME;
const s3 = new S3Client({ region: REGION });
const BASE_URL = "https://api.space-invaders.com";

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
  constructor() {
    super();
  }

  protected async handleMessage(msg: ConsumeMessage): Promise<void> {
    const content = msg.content.toString();
    const flash: Flash = JSON.parse(content);
    const imageUrl = BASE_URL + flash.img;
    const s3Key = flash.img.replace(/^\//, "");

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

      console.log(`[FlashConsumer] Successfully downloaded image (${contentLength || "unknown"} bytes, ${contentType})`);

      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
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

      // console.log(`[FlashConsumer] Uploaded image to S3: https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/${s3Key}`);

      // Add a small delay after successful processing to avoid overwhelming the server
      await sleep(Math.random() * 2000 + 500);
    } catch (err) {
      console.error("[FlashConsumer] Error downloading/uploading image:", err);

      // Log more detailed error information
      if (axios.isAxiosError(err)) {
        console.error(`[FlashConsumer] HTTP Status: ${err.response?.status}`);
        console.error(`[FlashConsumer] Response Headers:`, err.response?.headers);
        console.error(`[FlashConsumer] Request URL: ${err.config?.url}`);
      }

      throw err;
    }
  }
}

(async () => {
  const consumer = new FlashConsumer();
  await consumer.startConsuming();
})();
