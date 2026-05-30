import { describe, it, expect, onTestFinished } from 'vitest';
import { Readable } from 'stream';
import { mkdtemp, readFile, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { FakeDriver } from '../../src/drivers/FakeDriver';

function buildDriver({ prefix }: { prefix?: string | null } = { prefix: 'some/prefix' }): FakeDriver {
  const driver = new FakeDriver({ bucket: 'bucket', prefix: prefix ?? null });

  onTestFinished(() => driver.flush());

  return driver;
}

async function makeWorkDir(): Promise<string> {
  const workDir = await mkdtemp(join(tmpdir(), 'kraps-test-'));

  onTestFinished(async () => {
    await rm(workDir, { recursive: true, force: true });
  });

  return workDir;
}

describe('FakeDriver', () => {
  describe('withPrefix', () => {
    it('prepends the prefix', () => {
      expect(buildDriver().withPrefix('and/path')).toBe('some/prefix/and/path');
    });

    it('returns the path when no prefix is configured', () => {
      expect(buildDriver({ prefix: null }).withPrefix('and/path')).toBe('and/path');
    });

    it('normalizes a trailing slash on the prefix', () => {
      expect(buildDriver({ prefix: 'some/prefix/' }).withPrefix('and/path')).toBe('some/prefix/and/path');
    });
  });

  describe('flush', () => {
    it('clears all objects', async () => {
      const driver = buildDriver();

      await driver.store('path/to/object1', 'value1');
      await driver.store('path/to/object2', 'value2');

      driver.flush();

      expect(await driver.exists('path/to/object1')).toBe(false);
      expect(await driver.exists('path/to/object2')).toBe(false);
    });
  });

  describe('list', () => {
    it('returns sorted object names', async () => {
      const driver = buildDriver();

      await driver.store('some/path/to/object1', 'value1');
      await driver.store('other/path/to/object2', 'value2');

      expect(await driver.list()).toEqual(['other/path/to/object2', 'some/path/to/object1']);
    });

    it('respects the specified prefix', async () => {
      const driver = buildDriver();

      await driver.store('some/path/to/object1', 'value1');
      await driver.store('other/path/to/object2', 'value2');
      await driver.store('other/path/to/object3', 'value3');

      expect(await driver.list({ prefix: 'other/' })).toEqual([
        'other/path/to/object2',
        'other/path/to/object3',
      ]);
    });
  });

  describe('value', () => {
    it('returns the content of the object', async () => {
      const driver = buildDriver();

      await driver.store('path/to/object', 'value');

      expect(await driver.value('path/to/object')).toBe('value');
    });
  });

  describe('download', () => {
    it('downloads the object to the specified path', async () => {
      const driver = buildDriver();
      const workDir = await makeWorkDir();
      const path = join(workDir, 'out.txt');

      await driver.store('path/to/object', 'value');
      await driver.download('path/to/object', path);

      expect((await readFile(path)).toString('utf8')).toBe('value');
    });
  });

  describe('exists', () => {
    it('returns true when the object exists and false when not', async () => {
      const driver = buildDriver();

      await driver.store('path/to/object', 'value');

      expect(await driver.exists('path/to/object')).toBe(true);
      expect(await driver.exists('path/to/missing')).toBe(false);
    });
  });

  describe('store', () => {
    it('stores string data', async () => {
      const driver = buildDriver();

      await driver.store('path/to/object', 'value');

      expect(await driver.value('path/to/object')).toBe('value');
    });

    it('stores a readable stream', async () => {
      const driver = buildDriver();
      const stream = Readable.from(['hello ', 'world']);

      await driver.store('path/to/object', stream);

      expect(await driver.value('path/to/object')).toBe('hello world');
    });

    it('stores a buffer', async () => {
      const driver = buildDriver();

      await driver.store('path/to/object', Buffer.from('buffered'));

      expect(await driver.value('path/to/object')).toBe('buffered');
    });
  });
});
