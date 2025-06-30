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

class FlashConsumer extends RabbitMQBaseConsumer {
  constructor() {
    super("RABBITMQ_QUEUE");
  }

  protected async handleMessage(msg: ConsumeMessage): Promise<void> {
    const content = msg.content.toString();
    const flash: Flash = JSON.parse(content);
    const imageUrl = BASE_URL + flash.img;
    const s3Key = flash.img.replace(/^\//, "");
    console.log(`[FlashConsumer] Downloading image from: ${imageUrl}`);
    try {
      const response = await axios.get(imageUrl, { responseType: "arraybuffer" });
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
      console.log(`[FlashConsumer] Uploaded image to S3: ${imageUrl}`);
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
