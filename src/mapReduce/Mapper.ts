import { compare, JsonValue, KrapsKey } from './compare';
import { TempPath } from '../TempPath';
import { GzippedLineWriter, writeGzippedLines } from './lines';
import { InvalidChunkLimit } from '../errors';
import { kWayMerge, Pair } from './merge';
import { reduceChunk, Reducer } from './reduce';

export type MapImplementation = {
  map: (...args: any[]) => unknown | Promise<unknown>;
  reduce?: Reducer;
};

export type MapPartitioner = (key: KrapsKey) => number;
export type Collector = (key: KrapsKey, value?: JsonValue) => Promise<void>;

type BufferItem = [[number, KrapsKey], JsonValue];

export class Mapper {
  private readonly implementation: MapImplementation;
  private readonly partitioner: MapPartitioner;
  private readonly memoryLimit: number;

  private buffer: BufferItem[] = [];
  private bufferSize = 0;
  private chunks: TempPath[] = [];

  constructor(
    { implementation, partitioner, memoryLimit }:
    { implementation: MapImplementation, partitioner: MapPartitioner, memoryLimit: number }
  ) {
    this.implementation = implementation;
    this.partitioner = partitioner;
    this.memoryLimit = memoryLimit;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await Promise.all(this.chunks.map((chunk) => chunk.delete()));
    this.chunks = [];
  }

  async map(...args: any[]): Promise<void> {
    const collector: Collector = async (key, value) => {
      const partition = this.partitioner(key);
      const item: BufferItem = [[partition, key], value ?? null];

      this.buffer.push(item);
      this.bufferSize += JSON.stringify(item).length;

      if (this.bufferSize >= this.memoryLimit) await this.flushBuffer();
    };

    await this.implementation.map(...args, collector);
  }

  async shuffle(chunkLimit: number): Promise<Map<number, TempPath>> {
    if (chunkLimit < 2) throw new InvalidChunkLimit('Chunk limit must be >= 2');
    if (this.bufferSize > 0) await this.flushBuffer();

    try {
      const merged = kWayMerge(this.chunks, chunkLimit);
      const stream = this.implementation.reduce ? reduceChunk(merged, this.implementation.reduce) : merged;

      return await this.splitByPartition(stream);
    } finally {
      await Promise.all(this.chunks.map((chunk) => chunk.delete()));
      this.chunks = [];
    }
  }

  private async flushBuffer(): Promise<void> {
    const chunkBuffer = this.buffer;
    this.buffer = [];
    this.bufferSize = 0;

    chunkBuffer.sort((left, right) => compare(left[0], right[0]));

    const tempPath = await TempPath.create();

    try {
      const source: AsyncIterable<Pair> = (async function* () {
        for (const item of chunkBuffer) yield item as Pair;
      })();

      const stream = this.implementation.reduce ? reduceChunk(source, this.implementation.reduce) : source;

      await writeGzippedLines(tempPath.path, (async function* () {
        for await (const pair of stream) yield JSON.stringify(pair);
      })());
    } catch (error) {
      await tempPath.delete();
      throw error;
    }

    this.chunks.push(tempPath);
  }

  private async splitByPartition(stream: AsyncIterable<Pair>): Promise<Map<number, TempPath>> {
    const result = new Map<number, TempPath>();
    let currentPartition: number | null = null;
    let currentWriter: GzippedLineWriter | null = null;

    try {
      for await (const [composedKey, value] of stream) {
        const [partition, originalKey] = composedKey as [number, KrapsKey];

        if (partition !== currentPartition) {
          if (currentWriter) await currentWriter.close();

          const tempPath = await TempPath.create();
          result.set(partition, tempPath);
          currentPartition = partition;
          currentWriter = new GzippedLineWriter(tempPath.path);
        }

        await currentWriter!.write(JSON.stringify([originalKey, value]));
      }

      if (currentWriter) await currentWriter.close();

      return result;
    } catch (error) {
      if (currentWriter) await currentWriter.close().catch(() => undefined);
      await Promise.all(Array.from(result.values()).map((tempPath) => tempPath.delete()));
      throw error;
    }
  }
}
