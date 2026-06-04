import { describe, it, expect } from 'vitest';
import { Redis } from 'ioredis';
import { configure, findJob, getConfig } from '../src/config';
import { FakeDriver } from '../src/drivers/FakeDriver';
import { defineJob } from '../src/KrapsJob';

function buildDriver(): FakeDriver {
  return new FakeDriver({ bucket: 'bucket' });
}

function buildRedis(): Redis {
  return new Redis({ db: 15, lazyConnect: true });
}

describe('config', () => {
  it('sets all fields when fully specified', () => {
    const driver = buildDriver();
    const redis = buildRedis();
    const enqueuer = async () => {};

    configure({
      driver,
      redis,
      namespace: 'namespace',
      jobTtl: 100,
      showProgress: true,
      enqueuer,
    });

    const config = getConfig();

    expect(config.driver).toBe(driver);
    expect(config.redis).toBe(redis);
    expect(config.namespace).toBe('namespace');
    expect(config.jobTtl).toBe(100);
    expect(config.showProgress).toBe(true);
    expect(config.enqueuer).toBe(enqueuer);
  });

  it('applies the default jobTtl of 4 days when not specified', () => {
    configure({ driver: buildDriver(), redis: buildRedis(), enqueuer: async () => {} });

    expect(getConfig().jobTtl).toBe(4 * 24 * 60 * 60);
  });

  it('resets unspecified fields to defaults on every call', () => {
    const driver = buildDriver();
    const redis = buildRedis();

    configure({ driver, redis, namespace: 'temporary', jobTtl: 42, showProgress: false, enqueuer: async () => {} });
    configure({ driver, redis, enqueuer: async () => {} });

    const config = getConfig();

    expect(config.namespace).toBe(null);
    expect(config.jobTtl).toBe(4 * 24 * 60 * 60);
    expect(config.showProgress).toBe(true);
  });

  describe('jobs', () => {
    const Good = defineJob({
      name: 'Good',
      job() {
        return null as any;
      },
    });

    it('stores the array and finds jobs by name', () => {
      configure({ driver: buildDriver(), redis: buildRedis(), jobs: [Good], enqueuer: async () => {} });

      expect(getConfig().jobs).toEqual([Good]);
      expect(findJob('Good')).toBe(Good);
      expect(findJob('Missing')).toBeUndefined();
    });

    it('throws when a job is missing a name', () => {
      const Bad = { job() { return null as any; } };

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobs: [Bad as any],
        enqueuer: async () => {},
      })).toThrow(/missing a name/);
    });

    it('throws when name is an empty string', () => {
      const Empty = defineJob({
        name: '',
        job() { return null as any; },
      });

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobs: [Empty],
        enqueuer: async () => {},
      })).toThrow(/non-empty string/);
    });

    it('throws when two jobs share a name', () => {
      const A = defineJob({ name: 'Same', job() { return null as any; } });
      const B = defineJob({ name: 'Same', job() { return null as any; } });

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobs: [A, B],
        enqueuer: async () => {},
      })).toThrow(/duplicate job name "Same"/);
    });
  });
});
