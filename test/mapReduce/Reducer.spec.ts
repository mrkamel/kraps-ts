import { describe, it, expect } from 'vitest';
import { writeFile, access } from 'fs/promises';
import { InvalidChunkLimit } from '../../src/errors';
import { Reducer } from '../../src/mapReduce/Reducer';
import { Pair } from '../../src/mapReduce/merge';
import { gzipPairLines } from '../helpers/chunks';

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function stageChunk(reducer: Reducer, pairs: Pair[]): Promise<string> {
  const chunkPath = await reducer.addChunk();

  await writeFile(chunkPath, gzipPairLines(pairs));

  return chunkPath;
}

async function collect(stream: AsyncIterable<Pair>): Promise<Pair[]> {
  const result: Pair[] = [];

  for await (const pair of stream) result.push(pair);

  return result;
}

describe('Reducer', () => {
  it('reduces adjacent same-key pairs across a single chunk', async () => {
    const reducer = new Reducer({
      implementation: { reduce: (_key, leftValue, rightValue) => (leftValue as number) + (rightValue as number) },
    });

    await stageChunk(reducer, [
      ['apple', 1],
      ['apple', 2],
      ['apple', 3],
      ['banana', 10],
    ]);

    const result = await collect(reducer.reduce(4));

    expect(result).toEqual([
      ['apple', 6],
      ['banana', 10],
    ]);
  });

  it('merges multiple chunks and reduces across them', async () => {
    const reducer = new Reducer({
      implementation: { reduce: (_key, leftValue, rightValue) => (leftValue as number) + (rightValue as number) },
    });

    await stageChunk(reducer, [['apple', 1], ['cherry', 5]]);
    await stageChunk(reducer, [['apple', 2], ['banana', 3]]);
    await stageChunk(reducer, [['banana', 4], ['cherry', 6]]);

    const result = await collect(reducer.reduce(4));

    expect(result).toEqual([
      ['apple', 3],
      ['banana', 7],
      ['cherry', 11],
    ]);
  });

  it('respects chunkLimit by spilling intermediate reduced chunks', async () => {
    const reducer = new Reducer({
      implementation: { reduce: (_key, leftValue, rightValue) => (leftValue as number) + (rightValue as number) },
    });

    await stageChunk(reducer, [['key', 1]]);
    await stageChunk(reducer, [['key', 2]]);
    await stageChunk(reducer, [['key', 4]]);
    await stageChunk(reducer, [['key', 8]]);
    await stageChunk(reducer, [['key', 16]]);

    const result = await collect(reducer.reduce(2));

    expect(result).toEqual([['key', 31]]);
  });

  it('deletes all chunks after reducing', async () => {
    const reducer = new Reducer({
      implementation: { reduce: (_key, leftValue, rightValue) => (leftValue as number) + (rightValue as number) },
    });

    const chunkPath1 = await stageChunk(reducer, [['apple', 1]]);
    const chunkPath2 = await stageChunk(reducer, [['banana', 2]]);

    await collect(reducer.reduce(4));

    expect(await exists(chunkPath1)).toBe(false);
    expect(await exists(chunkPath2)).toBe(false);
  });

  it('yields nothing when no chunks were added', async () => {
    const reducer = new Reducer({
      implementation: { reduce: () => null },
    });

    const result = await collect(reducer.reduce(4));

    expect(result).toEqual([]);
  });

  it('throws InvalidChunkLimit when chunkLimit is less than 2', async () => {
    const reducer = new Reducer({
      implementation: { reduce: () => null },
    });

    await expect(collect(reducer.reduce(1))).rejects.toThrow(InvalidChunkLimit);
  });
});
