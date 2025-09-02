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
  
  // Override this to return true if message should be requeued on failure
  protected shouldRequeueOnFailure(error: Error): boolean {
    return false; // Default: don't requeue
  }

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
              const shouldRequeue = this.shouldRequeueOnFailure(err as Error);
              if (shouldRequeue) {
                console.log("[RabbitMQBaseConsumer] Requeuing message for retry");
                channel.nack(msg, false, true); // Requeue for retry
              } else {
                console.log("[RabbitMQBaseConsumer] Not requeuing - message will be discarded");
                channel.nack(msg, false, false); // Don't requeue
              }
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
