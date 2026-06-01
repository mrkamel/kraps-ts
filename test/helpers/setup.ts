import { onTestFinished } from 'vitest';
import { Redis } from 'ioredis';
import { configure, ConfigureOptions, JobClassRegistry } from '../../src/config';
import { FakeDriver } from '../../src/drivers/FakeDriver';
import { Worker } from '../../src/Worker';

const REDIS_DB = 15;

export type SetupOverrides = Omit<ConfigureOptions, 'driver' | 'redis' | 'jobClasses'>;

export async function setupKraps(overrides: SetupOverrides = {}): Promise<{
  driver: FakeDriver;
  redis: Redis;
  jobClasses: JobClassRegistry;
}> {
  const driver = new FakeDriver({ bucket: 'bucket', prefix: 'prefix' });
  const redis = new Redis({ db: REDIS_DB });
  const jobClasses: JobClassRegistry = {};

  await redis.flushdb();

  configure({ driver, redis, jobClasses, ...overrides });

  onTestFinished(async () => {
    driver.flush();
    await redis.flushdb();
    await redis.quit();
  });

  return { driver, redis, jobClasses };
}

export function inlineWorkerEnqueuer(): ConfigureOptions['enqueuer'] {
  return async (_worker, json) => {
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
