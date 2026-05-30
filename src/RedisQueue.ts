import type { Redis } from 'ioredis';
import { Interval } from './Interval';

const VISIBILITY_TIMEOUT_SECONDS = 60;
const KEEP_ALIVE_INTERVAL_MILLIS = 5_000;

export class RedisQueue {
  readonly redis: Redis;
  readonly token: string;
  readonly namespace: string | null;
  readonly ttl: number;

  constructor({ redis, token, namespace, ttl }: { redis: Redis, token: string, namespace: string | null, ttl: number }) {
    this.redis = redis;
    this.token = token;
    this.namespace = namespace;
    this.ttl = ttl;
  }

  async size(): Promise<number> {
    const script = `
      local queue_key, pending_key, status_key, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)

      return redis.call('llen', queue_key) + redis.call('zcard', pending_key)
    `;

    const result = await this.redis.eval(script, 0, this.queueKey, this.pendingKey, this.statusKey, this.ttl);

    return Number(result);
  }

  async enqueue(payload: unknown): Promise<void> {
    const script = `
      local queue_key, pending_key, status_key, ttl, job = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]

      redis.call('rpush', queue_key, job)

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)
    `;

    await this.redis.eval(script, 0, this.queueKey, this.pendingKey, this.statusKey, this.ttl, JSON.stringify(payload));
  }

  async dequeue<T>(handler: (payload: unknown | null) => Promise<T>): Promise<T | undefined> {
    const dequeueScript = `
      local queue_key, pending_key, status_key, ttl, visibility_timeout = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), tonumber(ARGV[5])

      local zitem = redis.call('zrange', pending_key, 0, 0, 'WITHSCORES')
      local job = zitem[1]

      if not zitem[2] or tonumber(zitem[2]) > tonumber(redis.call('time')[1]) then
        job = redis.call('lpop', queue_key)
      end

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)

      if not job then return nil end

      redis.call('zadd', pending_key, tonumber(redis.call('time')[1]) + visibility_timeout, job)
      redis.call('expire', pending_key, ttl)

      return job
    `;

    const raw = await this.redis.eval(
      dequeueScript, 0,
      this.queueKey, this.pendingKey, this.statusKey, this.ttl, VISIBILITY_TIMEOUT_SECONDS,
    ) as string | null;

    if (!raw) {
      await handler(null);
      return undefined;
    }

    const payload = JSON.parse(raw);
    const interval = new Interval(KEEP_ALIVE_INTERVAL_MILLIS, () => this.keepAlive(raw));

    let result: T;

    try {
      result = await handler(payload);
    } finally {
      await interval.stop();
    }

    const removeScript = `
      local queue_key, pending_key, status_key, ttl, job = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5]

      redis.call('zrem', pending_key, job)

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)
    `;

    await this.redis.eval(removeScript, 0, this.queueKey, this.pendingKey, this.statusKey, this.ttl, raw);

    return result;
  }

  async stop(): Promise<void> {
    const script = `
      local queue_key, pending_key, status_key, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

      redis.call('hset', status_key, 'stopped', 1)

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)
    `;

    await this.redis.eval(script, 0, this.queueKey, this.pendingKey, this.statusKey, this.ttl);
  }

  async stopped(): Promise<boolean> {
    const script = `
      local queue_key, pending_key, status_key, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)

      return redis.call('hget', status_key, 'stopped')
    `;

    const result = await this.redis.eval(script, 0, this.queueKey, this.pendingKey, this.statusKey, this.ttl) as string | null;

    return result === '1';
  }

  private async keepAlive(rawJob: string): Promise<void> {
    const script = `
      local queue_key, pending_key, status_key, ttl, job, visibility_timeout = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5], tonumber(ARGV[6])

      redis.call('zadd', pending_key, tonumber(redis.call('time')[1]) + visibility_timeout, job)

      redis.call('expire', queue_key, ttl)
      redis.call('expire', pending_key, ttl)
      redis.call('expire', status_key, ttl)
    `;

    await this.redis.eval(
      script, 0,
      this.queueKey, this.pendingKey, this.statusKey, this.ttl, rawJob, VISIBILITY_TIMEOUT_SECONDS,
    );
  }

  private get queueKey(): string {
    return [this.namespace, 'kraps', 'queue', this.token].filter((part): part is string => Boolean(part)).join(':');
  }

  private get pendingKey(): string {
    return [this.namespace, 'kraps', 'pending', this.token].filter((part): part is string => Boolean(part)).join(':');
  }

  private get statusKey(): string {
    return [this.namespace, 'kraps', 'status', this.token].filter((part): part is string => Boolean(part)).join(':');
  }
}
