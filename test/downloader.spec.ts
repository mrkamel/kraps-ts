import { describe, it, expect, beforeEach } from 'vitest';
import { readFile } from 'fs/promises';
import { downloadAll } from '../src/downloader';
import { setupKraps } from './helpers/setup';

describe('downloadAll', () => {
  beforeEach(async () => {
    await setupKraps();
  });

  it('downloads the available files and returns the temp paths', async () => {
    const { driver } = await setupKraps();

    await driver.store('path/to/chunk1.json', 'chunk1');
    await driver.store('path/to/chunk2.json', 'chunk2');
    await driver.store('path/to/chunk3.json', 'chunk3');

    await using tempPaths = await downloadAll({ prefix: 'path/to/', concurrency: 2 });

    const contents = await Promise.all(
      tempPaths.toArray().map(async (tempPath) => (await readFile(tempPath.path)).toString('utf8')),
    );

    expect(contents).toEqual(['chunk1', 'chunk2', 'chunk3']);
  });

  it('returns no temp paths when there are no files to download', async () => {
    const tempPaths = await downloadAll({ prefix: 'some/unknown/path/', concurrency: 4 });

    expect(tempPaths.toArray()).toEqual([]);
  });
});
