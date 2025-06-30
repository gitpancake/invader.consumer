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
];

// Function to get a random user agent
function getRandomUserAgent(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

class FlashConsumer extends RabbitMQBaseConsumer {
  constructor() {
    super("RABBITMQ_QUEUE");
  }

  protected async handleMessage(msg: ConsumeMessage): Promise<void> {
    const content = msg.content.toString();
    const flash: Flash = JSON.parse(content);
    const imageUrl = BASE_URL + flash.img;
    const s3Key = flash.img.replace(/^\//, "");
    try {
      const response = await axios.get(imageUrl, {
        responseType: "arraybuffer",
        headers: {
          "User-Agent": getRandomUserAgent(),
          Accept: "image/webp,image/apng,image/*,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9",
          "Accept-Encoding": "gzip, deflate, br",
          DNT: "1",
          Connection: "keep-alive",
          "Upgrade-Insecure-Requests": "1",
          "Sec-Fetch-Dest": "image",
          "Sec-Fetch-Mode": "no-cors",
          "Sec-Fetch-Site": "cross-site",
          "Cache-Control": "no-cache",
          Pragma: "no-cache",
        },
        timeout: 30000,
      });
      const contentType = response.headers["content-type"] || "image/jpeg";
      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
          Body: response.data,
          ContentType: contentType,
          ACL: "public-read",
        })
      );
      console.log(`[FlashConsumer] Uploaded image to S3: https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/${s3Key}`);
    } catch (err) {
      console.error("[FlashConsumer] Error downloading/uploading image:", err);
      throw err;
    }
  }
}

(async () => {
  const consumer = new FlashConsumer();
  await consumer.startConsuming();
})();
