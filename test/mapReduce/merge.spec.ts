import { describe, it, expect } from 'vitest';
import { writeFile, readFile } from 'fs/promises';
import { InvalidChunkLimit } from '../../src/errors';
import { kWayMerge, Pair } from '../../src/mapReduce/merge';
import { TempPath } from '../../src/TempPath';
import { gzipPairLines, gunzipPairLines } from '../helpers/chunks';

async function makeChunk(pairs: Pair[]): Promise<TempPath> {
  const tempPath = await TempPath.create();

  await writeFile(tempPath.path, gzipPairLines(pairs));

  return tempPath;
}

async function collect(stream: AsyncIterable<Pair>): Promise<Pair[]> {
  const result: Pair[] = [];

  for await (const pair of stream) result.push(pair);

  return result;
}

describe('kWayMerge', () => {
  it('yields nothing when no files are provided', async () => {
    const result = await collect(kWayMerge([], 4));

    expect(result).toEqual([]);
  });

  it('yields all pairs from a single file in order', async () => {
    const chunk = await makeChunk([['a', 1], ['b', 2], ['c', 3]]);

    try {
      const result = await collect(kWayMerge([chunk], 4));

      expect(result).toEqual([['a', 1], ['b', 2], ['c', 3]]);
    } finally {
      await chunk.delete();
    }
  });

  it('merges two sorted files keeping global key order', async () => {
    const chunk1 = await makeChunk([['a', 1], ['c', 3], ['e', 5]]);
    const chunk2 = await makeChunk([['b', 2], ['d', 4], ['f', 6]]);

    try {
      const result = await collect(kWayMerge([chunk1, chunk2], 4));

      expect(result).toEqual([
        ['a', 1],
        ['b', 2],
        ['c', 3],
        ['d', 4],
        ['e', 5],
        ['f', 6],
      ]);
    } finally {
      await chunk1.delete();
      await chunk2.delete();
    }
  });

  it('interleaves equal-key entries from multiple files via the priority queue tie-breaker', async () => {
    const chunk1 = await makeChunk([['a', 1], ['a', 3]]);
    const chunk2 = await makeChunk([['a', 2], ['a', 4]]);

    try {
      const result = await collect(kWayMerge([chunk1, chunk2], 4));

      expect(result).toEqual([
        ['a', 1],
        ['a', 2],
        ['a', 3],
        ['a', 4],
      ]);
    } finally {
      await chunk1.delete();
      await chunk2.delete();
    }
  });

  it('compares array keys element-wise', async () => {
    const chunk1 = await makeChunk([[[0, 'a'], 1], [[1, 'a'], 2]]);
    const chunk2 = await makeChunk([[[0, 'b'], 3], [[1, 'b'], 4]]);

    try {
      const result = await collect(kWayMerge([chunk1, chunk2], 4));

      expect(result).toEqual([
        [[0, 'a'], 1],
        [[0, 'b'], 3],
        [[1, 'a'], 2],
        [[1, 'b'], 4],
      ]);
    } finally {
      await chunk1.delete();
      await chunk2.delete();
    }
  });

  it('handles more input files than chunkLimit via intermediate merges', async () => {
    const chunks = await Promise.all([
      makeChunk([['a', 1]]),
      makeChunk([['b', 2]]),
      makeChunk([['c', 3]]),
      makeChunk([['d', 4]]),
      makeChunk([['e', 5]]),
    ]);

    try {
      const result = await collect(kWayMerge(chunks, 2));

      expect(result).toEqual([
        ['a', 1],
        ['b', 2],
        ['c', 3],
        ['d', 4],
        ['e', 5],
      ]);
    } finally {
      await Promise.all(chunks.map((chunk) => chunk.delete()));
    }
  });

  it('does not modify the input files', async () => {
    const chunk = await makeChunk([['a', 1], ['b', 2]]);
    const before = await readFile(chunk.path);

    try {
      await collect(kWayMerge([chunk], 4));

      const after = await readFile(chunk.path);

      expect(after.equals(before)).toBe(true);
      expect(gunzipPairLines(after)).toEqual([['a', 1], ['b', 2]]);
    } finally {
      await chunk.delete();
    }
  });

  it('throws InvalidChunkLimit when chunkLimit is less than 2', async () => {
    await expect(collect(kWayMerge([], 1))).rejects.toThrow(InvalidChunkLimit);
  });
});
