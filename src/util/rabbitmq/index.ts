import { connect, ConsumeMessage } from "amqplib";

/**
 * Usage: Extend this class and implement handleMessage(msg, channel) using this.contractService
 */
export abstract class RabbitMQBaseConsumer {
  protected rabbitUrl: string;
  protected queue: string;

  constructor() {
    this.rabbitUrl = process.env.RABBITMQ_URL!;
    this.queue = process.env.RABBITMQ_QUEUE!;
    

    if (!this.rabbitUrl) throw new Error("RABBITMQ_URL is not defined in the environment variables");
    if (!this.queue) throw new Error(`RABBITMQ_QUEUE is not defined in the environment variables`);
  }

  protected abstract handleMessage(msg: ConsumeMessage): Promise<void>;

  public async startConsuming(testMode: boolean = false) {
    const connection = await connect(this.rabbitUrl);
    const channel = await connection.createChannel();
    await channel.assertQueue(this.queue, { durable: true });
    
    if (testMode) {
      console.log(`[RabbitMQBaseConsumer] Running in TEST MODE - messages will NOT be removed from queue`);
    } else {
      console.log(`[RabbitMQBaseConsumer] Waiting for messages in ${this.queue}. To exit press CTRL+C`);
    }
    
    channel.consume(
      this.queue,
      async (msg) => {
        if (msg) {
          try {
            await this.handleMessage(msg);
            if (!testMode) {
              channel.ack(msg);
            } else {
              // In test mode, reject message and requeue it
              channel.nack(msg, false, true);
              console.log(`[RabbitMQBaseConsumer] TEST MODE: Message requeued`);
            }
          } catch (err) {
            console.error("[RabbitMQBaseConsumer] Error processing message:", err);
            if (!testMode) {
              channel.nack(msg, false, false);
            } else {
              // In test mode, still requeue even on error
              channel.nack(msg, false, true);
            }
          }
        }
      },
      { noAck: false }
    );
  }
}
