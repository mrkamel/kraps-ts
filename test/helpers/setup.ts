import { onTestFinished } from 'vitest';
import { Redis } from 'ioredis';
import { configure, ConfigureOptions } from '../../src/config';
import { FakeDriver } from '../../src/drivers/FakeDriver';
import { Worker } from '../../src/Worker';

const REDIS_DB = 15;

export type SetupOverrides = Partial<ConfigureOptions>;

export async function setupKraps(overrides: SetupOverrides = {}): Promise<{
  driver: FakeDriver;
  redis: Redis;
}> {
  const driver = new FakeDriver({ bucket: 'bucket', prefix: 'prefix' });
  const redis = new Redis({ db: REDIS_DB });

  await redis.flushdb();

  configure({
    enqueuer: inlineWorkerEnqueuer(),
    driver,
    redis,
    ...overrides,
  });

  onTestFinished(async () => {
    driver.flush();
    await redis.flushdb();
    await redis.quit();
  });

  return { driver, redis };
}

export function inlineWorkerEnqueuer(): ConfigureOptions['enqueuer'] {
  return async (json) => {
    const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });

    await worker.run({ retries: 0 });
  };
}

export async function buildRedis(): Promise<Redis> {
  const redis = new Redis({ db: REDIS_DB });

  await redis.flushdb();

  onTestFinished(async () => {
    await redis.flushdb();
    await redis.quit();
  });

  return redis;
}
