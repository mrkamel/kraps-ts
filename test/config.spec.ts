import { describe, it, expect } from 'vitest';
import { Redis } from 'ioredis';
import { configure, findJobClass, findJobName, getConfig } from '../src/config';
import { FakeDriver } from '../src/drivers/FakeDriver';

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
    class Good {
      run(): any {
        return null;
      }
    }

    it('stores the dict and resolves classes/names in both directions', () => {
      configure({ driver: buildDriver(), redis: buildRedis(), jobs: { Good }, enqueuer: async () => {} });

      expect(getConfig().jobs).toEqual({ Good });
      expect(findJobClass('Good')).toBe(Good);
      expect(findJobClass('Missing')).toBeUndefined();
      expect(findJobName(Good)).toBe('Good');
    });

    it('throws when name is an empty string', () => {
      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobs: { '': Good },
        enqueuer: async () => {},
      })).toThrow(/non-empty string/);
    });

    it('throws when the same class is registered under two names', () => {
      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobs: { Good, GoodAlias: Good },
        enqueuer: async () => {},
      })).toThrow(/registered under two names/);
    });
  });
});
