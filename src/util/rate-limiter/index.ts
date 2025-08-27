export class RateLimiter {
  private lastRequestTime: number = 0;
  private requestCount: number = 0;
  private windowStart: number = Date.now();

  constructor(private maxRequestsPerSecond: number = 1, private maxRequestsPerMinute: number = 30) {}

  async waitIfNeeded(): Promise<void> {
    const now = Date.now();

    // Reset minute window if needed
    if (now - this.windowStart > 60000) {
      this.windowStart = now;
      this.requestCount = 0;
    }

    // Check if we've hit the minute limit
    if (this.requestCount >= this.maxRequestsPerMinute) {
      const waitTime = 60000 - (now - this.windowStart);
      if (waitTime > 0) {
        await this.sleep(waitTime);
        this.windowStart = Date.now();
        this.requestCount = 0;
      }
    }

    // Enforce per-second rate limit
    const timeSinceLastRequest = now - this.lastRequestTime;
    const minInterval = 1000 / this.maxRequestsPerSecond;

    if (timeSinceLastRequest < minInterval) {
      const waitTime = minInterval - timeSinceLastRequest;
      await this.sleep(waitTime);
    }

    this.lastRequestTime = Date.now();
    this.requestCount++;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  reset(): void {
    this.lastRequestTime = 0;
    this.requestCount = 0;
    this.windowStart = Date.now();
  }
}
