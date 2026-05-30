import { describe, it, expect } from 'vitest';
import { reduceChunk } from '../../src/mapReduce/reduce';
import { Pair } from '../../src/mapReduce/merge';

async function* fromArray(pairs: Pair[]): AsyncGenerator<Pair> {
  for (const pair of pairs) yield pair;
}

async function collect(stream: AsyncIterable<Pair>): Promise<Pair[]> {
  const result: Pair[] = [];
  for await (const pair of stream) result.push(pair);

  return result;
}

describe('reduceChunk', () => {
  it('collapses adjacent same-key pairs using the reducer', async () => {
    const input = fromArray([
      ['a', 1],
      ['a', 2],
      ['a', 3],
      ['b', 10],
      ['c', 100],
      ['c', 200],
    ]);

    const result = await collect(
      reduceChunk(input, (_key, left, right) => (left as number) + (right as number)),
    );

    expect(result).toEqual([
      ['a', 6],
      ['b', 10],
      ['c', 300],
    ]);
  });

  it('yields nothing for an empty chunk', async () => {
    const result = await collect(reduceChunk(fromArray([]), () => 0));

    expect(result).toEqual([]);
  });
});
