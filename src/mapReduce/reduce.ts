import { compare, JsonValue, KrapsKey } from './compare';
import { Pair } from './merge';

export type Reducer = (key: KrapsKey, leftValue: JsonValue, rightValue: JsonValue) => JsonValue | Promise<JsonValue>;

export async function* reduceChunk(chunk: AsyncIterable<Pair>, reducer: Reducer): AsyncGenerator<Pair> {
  let previous: Pair | null = null;

  for await (const current of chunk) {
    if (previous === null) {
      previous = current;
      continue;
    }

    if (compare(previous[0], current[0]) === 0) {
      const merged = await reducer(previous[0], previous[1], current[1]);
      previous = [previous[0], merged];
    } else {
      yield previous;
      previous = current;
    }
  }

  if (previous !== null) yield previous;
}
