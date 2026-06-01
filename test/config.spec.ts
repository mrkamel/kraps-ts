import { describe, it, expect } from 'vitest';
import { Redis } from 'ioredis';
import { configure, findJobClass, getConfig } from '../src/config';
import { FakeDriver } from '../src/drivers/FakeDriver';
import { KrapsJob } from '../src/KrapsJob';

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

    configure({
      driver,
      redis,
      namespace: 'namespace',
      jobTtl: 100,
      showProgress: true,
    });

    const config = getConfig();

    expect(config.driver).toBe(driver);
    expect(config.redis).toBe(redis);
    expect(config.namespace).toBe('namespace');
    expect(config.jobTtl).toBe(100);
    expect(config.showProgress).toBe(true);
  });

  it('applies the default jobTtl of 4 days when not specified', () => {
    configure({ driver: buildDriver(), redis: buildRedis() });

    expect(getConfig().jobTtl).toBe(4 * 24 * 60 * 60);
  });

  it('resets unspecified fields to defaults on every call', () => {
    const driver = buildDriver();
    const redis = buildRedis();

    configure({ driver, redis, namespace: 'temporary', jobTtl: 42, showProgress: false });
    configure({ driver, redis });

    const config = getConfig();

    expect(config.namespace).toBe(null);
    expect(config.jobTtl).toBe(4 * 24 * 60 * 60);
    expect(config.showProgress).toBe(true);
  });

  describe('jobClasses', () => {
    class Good implements KrapsJob {
      static jobName = 'Good';

      run(): any {
        return null;
      }
    }

    it('stores the array and finds classes by jobName', () => {
      configure({ driver: buildDriver(), redis: buildRedis(), jobClasses: [Good] });

      expect(getConfig().jobClasses).toEqual([Good]);
      expect(findJobClass('Good')).toBe(Good);
      expect(findJobClass('Missing')).toBeUndefined();
    });

    it('throws when a class is missing static jobName', () => {
      class Bad {
        run(): any {
          return null;
        }
      }

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobClasses: [Bad as any],
      })).toThrow(/missing a static jobName/);
    });

    it('throws when jobName is an empty string', () => {
      class Empty {
        static jobName = '';

        run(): any {
          return null;
        }
      }

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobClasses: [Empty],
      })).toThrow(/non-empty string/);
    });

    it('throws when two classes share a jobName', () => {
      class A {
        static jobName = 'Same';

        run(): any {
          return null;
        }
      }

      class B {
        static jobName = 'Same';

        run(): any {
          return null;
        }
      }

      expect(() => configure({
        driver: buildDriver(),
        redis: buildRedis(),
        jobClasses: [A, B],
      })).toThrow(/duplicate jobName "Same"/);
    });
  });
});
