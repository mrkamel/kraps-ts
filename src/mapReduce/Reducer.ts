import { TempPath } from '../TempPath';
import { InvalidChunkLimit } from '../errors';
import { writeGzippedLines } from './lines';
import { kWayMerge, Pair } from './merge';
import { reduceChunk, Reducer as ReducerFunction } from './reduce';

export type ReducerImplementation = {
  reduce: ReducerFunction,
};

export class Reducer {
  private readonly implementation: ReducerImplementation;
  private chunks: TempPath[] = [];

  constructor({ implementation }: { implementation: ReducerImplementation }) {
    this.implementation = implementation;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await Promise.all(this.chunks.map((chunk) => chunk.delete()));
    this.chunks = [];
  }

  async addChunk(): Promise<string> {
    const tempPath = await TempPath.create();
    this.chunks.push(tempPath);

    return tempPath.path;
  }

  async *reduce(chunkLimit: number): AsyncGenerator<Pair> {
    if (chunkLimit < 2) throw new InvalidChunkLimit('Chunk limit must be >= 2');

    try {
      while (true) {
        const slice = this.chunks.splice(0, chunkLimit);
        const merged = kWayMerge(slice, chunkLimit);
        const reduced = reduceChunk(merged, this.implementation.reduce);

        if (this.chunks.length === 0) {
          try {
            for await (const pair of reduced) yield pair;
          } finally {
            await Promise.all(slice.map((chunk) => chunk.delete()));
          }
          return;
        }

        const output = await TempPath.create();

        try {
          await writeGzippedLines(output.path, (async function* () {
            for await (const pair of reduced) yield JSON.stringify(pair);
          })());
        } finally {
          await Promise.all(slice.map((chunk) => chunk.delete()));
        }

        this.chunks.push(output);
      }
    } finally {
      await Promise.all(this.chunks.map((chunk) => chunk.delete()));
      this.chunks = [];
    }
  }
}
