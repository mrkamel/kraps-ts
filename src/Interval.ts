import { tryCatch } from './tryCatch';

export class Interval {
  private timer: NodeJS.Timeout | null;
  private running: Promise<void> = Promise.resolve();
  private stopped = false;

  constructor(intervalMillis: number, handler: () => void | Promise<void>) {
    this.timer = setInterval(() => {
      if (this.stopped) return;

      this.running = this.running.then(async () => {
        if (this.stopped) return;

        const [error] = await tryCatch(async () => {
          await handler();
        });

        if (error) console.error('kraps interval error:', error);
      });
    }, intervalMillis);
  }

  async stop(): Promise<void> {
    this.stopped = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    await this.running;
  }
}
