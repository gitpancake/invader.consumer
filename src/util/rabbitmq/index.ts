import { connect, ChannelModel, Channel, ConsumeMessage } from "amqplib";

/**
 * Usage: Extend this class and implement handleMessage(msg, channel) using this.contractService
 */
export abstract class RabbitMQBaseConsumer {
    protected rabbitUrl: string;
    protected queue: string;
    private connection: ChannelModel | null = null;
    private channel: Channel | null = null;
    private testMode: boolean = false;
    private reconnecting: boolean = false;
    private reconnectAttempts: number = 0;
    private static readonly MAX_RECONNECT_DELAY = 30000;
    private static readonly INITIAL_RECONNECT_DELAY = 1000;
    private static readonly HEARTBEAT_INTERVAL = 30;

    constructor() {
        this.rabbitUrl = process.env.RABBITMQ_URL!;
        this.queue = process.env.RABBITMQ_QUEUE!;

        if (!this.rabbitUrl)
            throw new Error(
                "RABBITMQ_URL is not defined in the environment variables",
            );
        if (!this.queue)
            throw new Error(
                `RABBITMQ_QUEUE is not defined in the environment variables`,
            );
    }

    protected abstract handleMessage(msg: ConsumeMessage): Promise<void>;

    // Override this to return true if message should be requeued on failure
    protected shouldRequeueOnFailure(error: Error): boolean {
        return false; // Default: don't requeue
    }

    private getReconnectDelay(): number {
        const delay = Math.min(
            RabbitMQBaseConsumer.INITIAL_RECONNECT_DELAY *
                Math.pow(2, this.reconnectAttempts),
            RabbitMQBaseConsumer.MAX_RECONNECT_DELAY,
        );
        return delay;
    }

    private async reconnect(): Promise<void> {
        if (this.reconnecting) return;
        this.reconnecting = true;

        while (true) {
            this.reconnectAttempts++;
            const delay = this.getReconnectDelay();
            console.log(
                `[RabbitMQBaseConsumer] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`,
            );
            await new Promise((resolve) => setTimeout(resolve, delay));

            try {
                await this.connect();
                this.reconnectAttempts = 0;
                this.reconnecting = false;
                console.log(`[RabbitMQBaseConsumer] Reconnected successfully`);
                return;
            } catch (err) {
                console.error(
                    `[RabbitMQBaseConsumer] Reconnect attempt ${this.reconnectAttempts} failed:`,
                    (err as Error).message,
                );
            }
        }
    }

    private async connect(): Promise<void> {
        // Connect with heartbeat to detect dead connections
        const url = new URL(this.rabbitUrl);
        url.searchParams.set(
            "heartbeat",
            String(RabbitMQBaseConsumer.HEARTBEAT_INTERVAL),
        );
        this.connection = await connect(url.toString());

        this.connection.on("error", (err) => {
            console.error(
                "[RabbitMQBaseConsumer] Connection error:",
                err.message,
            );
        });

        this.connection.on("close", () => {
            console.warn(
                "[RabbitMQBaseConsumer] Connection closed — initiating reconnect",
            );
            this.connection = null;
            this.channel = null;
            this.reconnect();
        });

        this.channel = await this.connection.createChannel();
        await this.channel.assertQueue(this.queue, { durable: true });

        const prefetchCount = parseInt(process.env.CONSUMER_CONCURRENCY || "1");
        await this.channel.prefetch(prefetchCount);

        this.channel.on("error", (err) => {
            console.error("[RabbitMQBaseConsumer] Channel error:", err.message);
        });

        this.channel.on("close", () => {
            console.warn("[RabbitMQBaseConsumer] Channel closed");
        });

        const channel = this.channel;
        const testMode = this.testMode;

        channel.consume(
            this.queue,
            async (msg) => {
                if (msg) {
                    try {
                        await this.handleMessage(msg);
                        if (!testMode) {
                            channel.ack(msg);
                        } else {
                            channel.nack(msg, false, true);
                            console.log(
                                `[RabbitMQBaseConsumer] TEST MODE: Message requeued`,
                            );
                        }
                    } catch (err) {
                        console.error(
                            "[RabbitMQBaseConsumer] Error processing message:",
                            err,
                        );
                        if (!testMode) {
                            const shouldRequeue = this.shouldRequeueOnFailure(
                                err as Error,
                            );
                            if (shouldRequeue) {
                                console.log(
                                    "[RabbitMQBaseConsumer] Requeuing message for retry",
                                );
                                channel.nack(msg, false, true);
                            } else {
                                console.log(
                                    "[RabbitMQBaseConsumer] Not requeuing - message will be discarded",
                                );
                                channel.nack(msg, false, false);
                            }
                        } else {
                            channel.nack(msg, false, true);
                        }
                    }
                }
            },
            { noAck: false },
        );
    }

    public async startConsuming(testMode: boolean = false) {
        this.testMode = testMode;
        await this.connect();

        const prefetchCount = parseInt(process.env.CONSUMER_CONCURRENCY || "1");
        const rateLimit = parseInt(process.env.CONSUMER_RATE_LIMIT || "250");

        if (testMode) {
            console.log(
                `[RabbitMQBaseConsumer] Running in TEST MODE - messages will NOT be removed from queue`,
            );
        } else {
            console.log(
                `[RabbitMQBaseConsumer] Processing messages with concurrency=${prefetchCount}, rate limit=${rateLimit} req/min in ${this.queue}. To exit press CTRL+C`,
            );
        }
    }
}
