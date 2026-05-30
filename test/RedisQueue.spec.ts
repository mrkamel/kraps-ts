import { describe, it, expect } from 'vitest';
import { Redis } from 'ioredis';
import { RedisQueue } from '../src/RedisQueue';
import { buildRedis } from './helpers/setup';

function buildRedisQueue(
  redis: Redis,
  options: { token?: string; namespace?: string | null; ttl?: number } = {},
): RedisQueue {
  return new RedisQueue({
    redis,
    token: options.token ?? 'token',
    namespace: options.namespace ?? null,
    ttl: options.ttl ?? 60,
  });
}

describe('RedisQueue', () => {
  describe('enqueue', () => {
    it('enqueues the specified payload', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      await queue.enqueue({ key: 'value1' });
      await queue.enqueue({ key: 'value2' });

      const entries = await redis.lrange('kraps:queue:token', 0, 10);

      expect(entries).toEqual([JSON.stringify({ key: 'value1' }), JSON.stringify({ key: 'value2' })]);
    });

    it('respects the specified namespace', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis, { namespace: 'namespace' });

      await queue.enqueue({ key: 'value' });

      const entries = await redis.lrange('namespace:kraps:queue:token', 0, 10);

      expect(entries).toEqual([JSON.stringify({ key: 'value' })]);
    });

    it('treats an empty-string namespace the same as null', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis, { namespace: '' });

      await queue.enqueue({ key: 'value' });

      const entries = await redis.lrange('kraps:queue:token', 0, 10);

      expect(entries).toEqual([JSON.stringify({ key: 'value' })]);
    });

    it('updates the expiry on every operation', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis, { ttl: 30 });

      await queue.enqueue({ key: 'value1' });
      await queue.enqueue({ key: 'value2' });
      await queue.stop();

      await queue.dequeue(async () => {
        await redis.persist('kraps:queue:token');
        await redis.persist('kraps:pending:token');
        await redis.persist('kraps:status:token');

        await queue.enqueue({ key: 'value3' });

        const queueTtl = await redis.ttl('kraps:queue:token');
        const pendingTtl = await redis.ttl('kraps:pending:token');
        const statusTtl = await redis.ttl('kraps:status:token');

        expect(queueTtl).toBeGreaterThanOrEqual(29);
        expect(queueTtl).toBeLessThanOrEqual(30);
        expect(pendingTtl).toBeGreaterThanOrEqual(29);
        expect(pendingTtl).toBeLessThanOrEqual(30);
        expect(statusTtl).toBeGreaterThanOrEqual(29);
        expect(statusTtl).toBeLessThanOrEqual(30);
      });
    });
  });

  describe('size', () => {
    it('returns the queue + pending size', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      expect(await queue.size()).toBe(0);

      await queue.enqueue({ key: 'value1' });
      expect(await queue.size()).toBe(1);

      await queue.enqueue({ key: 'value2' });
      expect(await queue.size()).toBe(2);

      await queue.dequeue(async () => {
        expect(await queue.size()).toBe(2);
      });

      expect(await queue.size()).toBe(1);
    });

    it('respects the namespace', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis, { namespace: 'namespace' });

      await queue.enqueue({ key: 'value1' });
      await queue.enqueue({ key: 'value2' });

      await queue.dequeue(async () => {
        expect(await queue.size()).toBe(2);
      });

      expect(await queue.size()).toBe(1);
    });
  });

  describe('dequeue', () => {
    it('dequeues a payload and removes it on success', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      await queue.enqueue({ key: 'value' });

      let received: unknown = null;

      expect(await redis.llen('kraps:queue:token')).toBe(1);
      expect(await redis.zcard('kraps:pending:token')).toBe(0);

      await queue.dequeue(async (payload) => {
        received = payload;
        expect(await redis.zcard('kraps:pending:token')).toBe(1);
      });

      expect(await redis.llen('kraps:queue:token')).toBe(0);
      expect(await redis.zcard('kraps:pending:token')).toBe(0);
      expect(received).toEqual({ key: 'value' });
    });

    it('calls the handler with null when no payload is available', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      let received: unknown = 'untouched';

      await queue.dequeue(async (payload) => {
        received = payload;
      });

      expect(received).toBeNull();
    });

    it('respects the namespace', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis, { namespace: 'namespace' });

      await queue.enqueue({ key: 'value' });

      let received: unknown = null;

      expect(await redis.llen('namespace:kraps:queue:token')).toBe(1);

      await queue.dequeue(async (payload) => {
        received = payload;
        expect(await redis.zcard('namespace:kraps:pending:token')).toBe(1);
      });

      expect(await redis.llen('namespace:kraps:queue:token')).toBe(0);
      expect(await redis.zcard('namespace:kraps:pending:token')).toBe(0);
      expect(received).toEqual({ key: 'value' });
    });

    it('keeps the payload in pending when the handler throws', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      await queue.enqueue({ key: 'value' });

      await expect(
        queue.dequeue(async () => { throw new Error('boom'); }),
      ).rejects.toThrow('boom');

      expect(await redis.llen('kraps:queue:token')).toBe(0);
      expect(await redis.zcard('kraps:pending:token')).toBe(1);
    });

    it('returns the pending job once its visibility timeout has elapsed', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      await queue.enqueue({ key: 'value1' });
      await queue.enqueue({ key: 'value2' });

      await expect(
        queue.dequeue(async () => { throw new Error('boom'); }),
      ).rejects.toThrow('boom');

      const [serverTime] = await redis.time();
      await redis.zadd('kraps:pending:token', String(Number(serverTime) - 1), JSON.stringify({ key: 'value1' }));

      let received: unknown = null;

      await queue.dequeue(async (payload) => { received = payload; });

      expect(received).toEqual({ key: 'value1' });
    });

    it('does not return a pending job that is still within its visibility window', async () => {
      const redis = await buildRedis();
      const queue = buildRedisQueue(redis);

      await queue.enqueue({ key: 'value1' });
      await queue.enqueue({ key: 'value2' });

      await expect(
        queue.dequeue(async () => { throw new Error('boom'); }),
      ).rejects.toThrow('boom');

      let received: unknown = null;

      await queue.dequeue(async (payload) => { received = payload; });

      expect(received).toEqual({ key: 'value2' });
    });
  });

  describe('stop', () => {
    it('sets the stopped flag', async () => {
      const queue = buildRedisQueue(await buildRedis());

      expect(await queue.stopped()).toBe(false);

      await queue.stop();

      expect(await queue.stopped()).toBe(true);
    });

    it('respects the namespace', async () => {
      const queue = buildRedisQueue(await buildRedis(), { namespace: 'namespace' });

      expect(await queue.stopped()).toBe(false);

      await queue.stop();

      expect(await queue.stopped()).toBe(true);
    });
  });
});
