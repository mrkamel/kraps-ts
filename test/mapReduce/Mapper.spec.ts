import { describe, it, expect } from 'vitest';
import { readFile } from 'fs/promises';
import { InvalidChunkLimit } from '../../src/errors';
import { Mapper } from '../../src/mapReduce/Mapper';
import { gunzipPairLines } from '../helpers/chunks';

async function readPartition(filePath: string): Promise<[unknown, unknown][]> {
  return gunzipPairLines(await readFile(filePath));
}

describe('Mapper', () => {
  describe('map + shuffle', () => {
    it('partitions emitted pairs and writes them sorted per partition', async () => {
      const mapper = new Mapper({
        implementation: {
          map: async (item: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
            await collector(item as string, 1);
          },
        },
        partitioner: (key) => ((key as string).charCodeAt(0) % 2),
        memoryLimit: 1024 * 1024,
      });

      for (const item of ['banana', 'apple', 'cherry', 'date']) {
        await mapper.map(item);
      }

      const partitions = await mapper.shuffle(8);

      try {
        const partition0 = await readPartition(partitions.get(0)!.path);
        const partition1 = await readPartition(partitions.get(1)!.path);

        expect(partition0).toEqual([['banana', 1], ['date', 1]]);
        expect(partition1).toEqual([['apple', 1], ['cherry', 1]]);
      } finally {
        await Promise.all(Array.from(partitions.values()).map((tempPath) => tempPath.delete()));
      }
    });

    it('pre-reduces adjacent same-key pairs when the implementation provides reduce', async () => {
      const mapper = new Mapper({
        implementation: {
          map: async (item: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
            await collector(item as string, 1);
            await collector(item as string, 1);
            await collector(item as string, 1);
          },
          reduce: (_key, leftValue, rightValue) => (leftValue as number) + (rightValue as number),
        },
        partitioner: () => 0,
        memoryLimit: 1024 * 1024,
      });

      await mapper.map('apple');
      await mapper.map('apple');
      await mapper.map('banana');

      const partitions = await mapper.shuffle(8);

      try {
        const partition0 = await readPartition(partitions.get(0)!.path);

        expect(partition0).toEqual([['apple', 6], ['banana', 3]]);
      } finally {
        await Promise.all(Array.from(partitions.values()).map((tempPath) => tempPath.delete()));
      }
    });

    it('spills intermediate chunks when the memory limit is exceeded', async () => {
      const mapper = new Mapper({
        implementation: {
          map: async (item: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
            await collector(item as string, 1);
          },
        },
        partitioner: () => 0,
        memoryLimit: 1,
      });

      for (const item of ['c', 'a', 'b', 'd', 'a']) {
        await mapper.map(item);
      }

      const partitions = await mapper.shuffle(8);

      try {
        const partition0 = await readPartition(partitions.get(0)!.path);

        expect(partition0).toEqual([
          ['a', 1],
          ['a', 1],
          ['b', 1],
          ['c', 1],
          ['d', 1],
        ]);
      } finally {
        await Promise.all(Array.from(partitions.values()).map((tempPath) => tempPath.delete()));
      }
    });

    it('spills mid-stream when a single map call emits more than memoryLimit (mapPartitions/combine pattern)', async () => {
      const mapper = new Mapper({
        implementation: {
          map: async (collector: (key: unknown, value?: unknown) => Promise<void>) => {
            for (let index = 0; index < 100; index++) await collector(`key-${index}`, index);
          },
        },
        partitioner: () => 0,
        memoryLimit: 32,
      });

      await mapper.map();

      const partitions = await mapper.shuffle(8);

      try {
        const partition0 = await readPartition(partitions.get(0)!.path);

        expect(partition0).toHaveLength(100);
      } finally {
        await Promise.all(Array.from(partitions.values()).map((tempPath) => tempPath.delete()));
      }
    });

    it('emits no partitions when no pairs are collected', async () => {
      const mapper = new Mapper({
        implementation: {
          map: () => undefined,
        },
        partitioner: () => 0,
        memoryLimit: 1024,
      });

      await mapper.map('ignored');

      const partitions = await mapper.shuffle(8);

      expect(partitions.size).toBe(0);
    });

    it('throws InvalidChunkLimit when chunkLimit is less than 2', async () => {
      const mapper = new Mapper({
        implementation: { map: () => undefined },
        partitioner: () => 0,
        memoryLimit: 1024,
      });

      await expect(mapper.shuffle(1)).rejects.toThrow(InvalidChunkLimit);
    });
  });
});
