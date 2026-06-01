import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JobStopped } from '../src/errors';
import { hashPartitioner, Partitioner } from '../src/hashPartitioner';
import { Job } from '../src/Job';
import { RedisQueue } from '../src/RedisQueue';
import { Runner } from '../src/Runner';
import { Worker } from '../src/Worker';
import { inlineWorkerEnqueuer, setupKraps } from './helpers/setup';

const KRAPS_WORKER = 'KrapsWorker';

describe('Runner end-to-end', () => {
  beforeEach(async () => {
    await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
  });

  it('runs parallelize → map → reduce → eachPartition end-to-end', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const store: Record<string, number> = {};

    class SearchCounter {
      static jobName = 'SearchCounter';

      run(): Job<string, number> {
        return new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 8 })
          .map(function* (key) {
            for (let times = 0; times < 3; times++) {
              yield [key, parseInt(key.replace('key', ''), 10)];
            }
          })
          .reduce((_key, left, right) => left + right)
          .eachPartition(async (_partition, pairs) => {
            for await (const [key, value] of pairs) store[key] = value;
          });
      }
    }

    jobClasses.push(SearchCounter);

    await new Runner(SearchCounter).run();

    expect(store).toEqual({
      key1: 3, key2: 6, key3: 9, key4: 12, key5: 15,
      key6: 18, key7: 21, key8: 24, key9: 27,
    });
  });

  it('passes positional arguments through to the job class', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const store: Record<string, number> = {};

    class Counter {
      static jobName = 'Counter';

      constructor(private readonly multiplier: number) {}

      run(): Job<string, number> {
        const multiplier = this.multiplier;

        return new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            for (let index = 1; index <= 3; index++) yield `key${index}`;
          }, { partitions: 4 })
          .map(function* (key) {
            yield [key, parseInt(key.replace('key', ''), 10) * multiplier];
          })
          .reduce((_key, left, right) => left + right)
          .eachPartition(async (_partition, pairs) => {
            for await (const [key, value] of pairs) store[key] = value;
          });
      }
    }

    jobClasses.push(Counter);

    await new Runner(Counter).run(2);

    expect(store).toEqual({ key1: 2, key2: 4, key3: 6 });
  });

  it('supports dump and load', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const collected: [number, [unknown, unknown][]][] = [];

    const partitioner: Partitioner<string> = hashPartitioner as Partitioner<string>;

    class DumpLoad {
      static jobName = 'DumpLoad';

      run(): [Job<string, number>, Job<string, number>] {
        const writeJob = new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 4 })
          .map(function* (key) {
            yield [key, parseInt(key.replace('key', ''), 10)];
          })
          .dump({ prefix: 'path/to/dump' });

        const readJob = new Job({ worker: KRAPS_WORKER })
          .load<string, number>({
            prefix: 'path/to/dump',
            partitions: 4,
            partitioner,
            concurrency: 4,
          })
          .eachPartition(async (partition, pairs) => {
            const list: [unknown, unknown][] = [];

            for await (const pair of pairs) list.push(pair);

            collected.push([partition, list]);
          });

        return [writeJob, readJob];
      }
    }

    jobClasses.push(DumpLoad);

    await new Runner(DumpLoad).run();

    collected.sort((leftEntry, rightEntry) => leftEntry[0] - rightEntry[0]);

    const allPairs = collected
      .flatMap(([, pairs]) => pairs)
      .sort((leftPair, rightPair) => (leftPair[0] as string).localeCompare(rightPair[0] as string));

    expect(allPairs).toEqual([
      ['key1', 1], ['key2', 2], ['key3', 3], ['key4', 4],
      ['key5', 5], ['key6', 6], ['key7', 7], ['key8', 8], ['key9', 9],
    ]);
  });

  it('does not affect the outcome when jobs is varied per step', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const store: Record<string, number> = {};

    class Counter {
      static jobName = 'Counter';

      run(): Job<string, number> {
        return new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 8 })
          .map(function* (key) {
            for (let times = 0; times < 3; times++) {
              yield [key, parseInt(key.replace('key', ''), 10)];
            }
          }, { jobs: 6 })
          .reduce((_key, left, right) => left + right, { jobs: 7 })
          .eachPartition(async (_partition, pairs) => {
            for await (const [key, value] of pairs) store[key] = value;
          }, { jobs: 3 });
      }
    }

    jobClasses.push(Counter);

    await new Runner(Counter).run();

    expect(store).toEqual({
      key1: 3, key2: 6, key3: 9, key4: 12, key5: 15,
      key6: 18, key7: 21, key8: 24, key9: 27,
    });
  });

  it('appends two jobs and yields the union of their pairs per partition', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const store: [string, number][] = [];

    class Appender {
      static jobName = 'Appender';

      run(): Job<string, number> {
        const leftJob = new Job({ worker: KRAPS_WORKER })
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3']) yield [item, 1] as [string, number];
          })
          .map(function* (key, value) {
            yield [key, value + 1];
          });

        const rightJob = new Job({ worker: KRAPS_WORKER })
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key3', 'key4', 'key5', 'key6']) yield [item, 2] as [string, number];
          })
          .map(function* (key, value) {
            yield [key, value + 1];
          });

        return leftJob.append(rightJob).eachPartition(async (_partition, pairs) => {
          for await (const pair of pairs) store.push(pair);
        });
      }
    }

    jobClasses.push(Appender);

    await new Runner(Appender).run();

    const sorted = [...store].sort((leftPair, rightPair) => {
      const keyOrder = leftPair[0].localeCompare(rightPair[0]);
      if (keyOrder !== 0) return keyOrder;
      return leftPair[1] - rightPair[1];
    });

    expect(sorted).toEqual([
      ['key1', 2],
      ['key2', 2],
      ['key3', 2],
      ['key3', 3],
      ['key4', 3],
      ['key5', 3],
      ['key6', 3],
    ]);
  });

  it('resolves recursive combine dependencies and omits keys missing from the joined side', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    const store: Record<string, number> = {};

    class Combiner {
      static jobName = 'Combiner';

      run(): Job<string, number> {
        const job1 = new Job({ worker: KRAPS_WORKER })
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3', 'key4', 'key5']) yield [item, 1] as [string, number];
          });

        const job2 = new Job({ worker: KRAPS_WORKER })
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3', 'key4']) yield [item, 2] as [string, number];
          })
          .combine<number, number>(job1, (key, leftValue, rightValue) =>
            [[key, leftValue + (rightValue ?? 0)]],
          );

        const job3 = new Job({ worker: KRAPS_WORKER })
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3']) yield [item, 3] as [string, number];
          })
          .combine<number, number>(job2, (key, leftValue, rightValue) =>
            [[key, leftValue + (rightValue ?? 0)]],
          );

        return job3.eachPartition(async (_partition, pairs) => {
          for await (const [key, value] of pairs) store[key] = value;
        });
      }
    }

    jobClasses.push(Combiner);

    await new Runner(Combiner).run();

    expect(store).toEqual({ key1: 6, key2: 6, key3: 6 });
  });

  it('does not execute shared steps more than once', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });
    let parallelizeCalls = 0;
    let mapCalls = 0;
    let reduceCalls = 0;

    class Shared {
      static jobName = 'Shared';

      run(): [Job<string, number>, Job<string, number>] {
        const reducedJob = new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            parallelizeCalls += 1;
            yield 'key';
          }, { partitions: 8 })
          .map(function* (key) {
            mapCalls += 1;
            yield [key, 1] as [string, number];
            yield [key, 1] as [string, number];
          })
          .mapPartitions(async function* (_partition, pairs) {
            for await (const [key, value] of pairs) yield [key, value];
          })
          .reduce((_key, leftValue, rightValue) => {
            reduceCalls += 1;
            return leftValue + rightValue;
          });

        const job1 = reducedJob.eachPartition(async () => undefined);
        const job2 = reducedJob.eachPartition(async () => undefined);

        return [job1, job2];
      }
    }

    jobClasses.push(Shared);

    await new Runner(Shared).run();

    expect(parallelizeCalls).toBe(1);
    expect(mapCalls).toBe(1);
    expect(reduceCalls).toBe(1);
  });

  it('invokes the configured enqueuer with the right payloads', async () => {
    const enqueuer = vi.fn(async (_worker: unknown, json: string) => {
      const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });

      await worker.run({ retries: 0 });
    });

    const { jobClasses } = await setupKraps({ enqueuer });

    class Pipeline {
      static jobName = 'Pipeline';

      run(): Job<string, number> {
        return new Job({ worker: KRAPS_WORKER })
          .parallelize(() => ['item1', 'item2'], { partitions: 4 })
          .map(function* (key) {
            yield [key, 1] as [string, number];
          }, { jobs: 3 })
          .reduce((_key, leftValue, rightValue) => leftValue + rightValue, { jobs: 2 });
      }
    }

    jobClasses.push(Pipeline);

    await new Runner(Pipeline).run();

    const calls = enqueuer.mock.calls.map(([, payload]) => JSON.parse(payload as string));

    const grouped = calls.reduce<Record<string, number>>((accumulator, payload) => {
      const key = `${payload.jobIndex}-${payload.stepIndex}`;

      accumulator[key] = (accumulator[key] ?? 0) + 1;
      return accumulator;
    }, {});

    expect(grouped['0-0']).toBe(2);
    expect(grouped['0-1']).toBe(3);
    expect(grouped['0-2']).toBe(2);
  });

  it('caps the number of background jobs by the number of partitions', async () => {
    const enqueuer = vi.fn(async (_worker: unknown, json: string) => {
      const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });

      await worker.run({ retries: 0 });
    });

    const { jobClasses } = await setupKraps({ enqueuer });

    class Capped {
      static jobName = 'Capped';

      run(): Job<string, number> {
        return new Job({ worker: KRAPS_WORKER })
          .parallelize(() => ['item1', 'item2'], { partitions: 4 })
          .map(function* (key) {
            yield [key, 1] as [string, number];
          }, { jobs: 8 });
      }
    }

    jobClasses.push(Capped);

    await new Runner(Capped).run();

    const mapCalls = enqueuer.mock.calls.filter(([, payload]) => {
      const parsed = JSON.parse(payload as string);
      return parsed.stepIndex === 1;
    });

    expect(mapCalls).toHaveLength(4);
  });

  it('throws JobStopped when the redis queue is stopped before the runner finishes', async () => {
    const { jobClasses } = await setupKraps({ enqueuer: inlineWorkerEnqueuer() });

    const stoppedSpy = vi.spyOn(RedisQueue.prototype, 'stopped').mockResolvedValue(true);

    class Stoppable {
      static jobName = 'Stoppable';

      run(): Job<string, null> {
        return new Job({ worker: KRAPS_WORKER })
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 8 })
          .map<string, null>(() => []);
      }
    }

    jobClasses.push(Stoppable);

    await expect(new Runner(Stoppable).run()).rejects.toThrow(JobStopped);

    stoppedSpy.mockRestore();
  });
});
