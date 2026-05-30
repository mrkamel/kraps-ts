import { InvalidChunkLimit } from '../errors';
import { TempPath } from '../TempPath';
import { TempPaths } from '../TempPaths';
import { JsonValue, KrapsKey } from './compare';
import { readGzippedLines, writeGzippedLines } from './lines';
import { PriorityQueue } from './PriorityQueue';

export type Pair = [KrapsKey, JsonValue];

async function* mergeSortedLineSources(sources: AsyncGenerator<string>[]): AsyncGenerator<Pair> {
  const queue = new PriorityQueue<{ index: number, key: KrapsKey, value: JsonValue }>();

  for (let index = 0; index < sources.length; index++) {
    const next = await sources[index].next();
    if (next.done) continue;

    const [key, value] = JSON.parse(next.value) as Pair;
    queue.push({ index, key, value }, key);
  }

  while (queue.size > 0) {
    const item = queue.pop()!;
    yield [item.key, item.value];

    const next = await sources[item.index].next();
    if (next.done) continue;

    const [key, value] = JSON.parse(next.value) as Pair;
    queue.push({ index: item.index, key, value }, key);
  }
}

class LineSources implements AsyncDisposable {
  private constructor(readonly generators: AsyncGenerator<string>[]) {}

  static open(tempPaths: TempPath[]): LineSources {
    return new LineSources(tempPaths.map((tempPath) => readGzippedLines(tempPath.path)));
  }

  async [Symbol.asyncDispose](): Promise<void> {
    for (const source of this.generators) await source.return(undefined);
  }
}

export async function* kWayMerge(tempPaths: TempPath[], chunkLimit: number): AsyncGenerator<Pair> {
  if (chunkLimit < 2) throw new InvalidChunkLimit('Chunk limit must be >= 2');

  const queue = [...tempPaths];
  await using intermediates = new TempPaths();

  while (queue.length > chunkLimit) {
    const slice = queue.splice(0, chunkLimit);
    const output = await intermediates.add();

    await using sources = LineSources.open(slice);

    await writeGzippedLines(output.path, (async function* () {
      for await (const pair of mergeSortedLineSources(sources.generators)) {
        yield JSON.stringify(pair);
      }
    })());

    queue.push(output);
  }

  await using sources = LineSources.open(queue);

  for await (const pair of mergeSortedLineSources(sources.generators)) {
    yield pair;
  }
}
