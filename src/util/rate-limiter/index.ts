export class RateLimiter {
  private queue: Array<() => void> = [];
  private processing = false;
  private requestsInWindow: number[] = [];

  constructor(
    private maxRequestsPerMinute: number = 250,
    private windowMs: number = 60000
  ) {}

  async waitIfNeeded(): Promise<void> {
    return new Promise((resolve) => {
      this.queue.push(resolve);
      this.processQueue();
    });
  }

  private async processQueue(): Promise<void> {
    if (this.processing || this.queue.length === 0) {
      return;
    }

    this.processing = true;

    while (this.queue.length > 0) {
      const now = Date.now();

      // Remove requests older than the window
      this.requestsInWindow = this.requestsInWindow.filter(
        time => now - time < this.windowMs
      );

      // If we're at the limit, wait until the oldest request expires
      if (this.requestsInWindow.length >= this.maxRequestsPerMinute) {
        const oldestRequest = this.requestsInWindow[0];
        const waitTime = this.windowMs - (now - oldestRequest) + 100; // +100ms buffer
        await this.sleep(waitTime);
        continue;
      }

      // Process next request
      this.requestsInWindow.push(Date.now());
      const resolve = this.queue.shift()!;
      resolve();

      // Small delay to prevent thundering herd
      await this.sleep(10);
    }

    this.processing = false;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  reset(): void {
    this.requestsInWindow = [];
    this.queue = [];
    this.processing = false;
  }
}
