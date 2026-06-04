import { describe, it, expect, vi } from 'vitest';
import { createJob } from '../src/createJob';
import { JobStopped } from '../src/errors';
import { hashPartitioner, Partitioner } from '../src/hashPartitioner';
import { Job } from '../src/Job';
import { defineJob } from '../src/KrapsJob';
import { RedisQueue } from '../src/RedisQueue';
import { Worker } from '../src/Worker';
import { setupKraps } from './helpers/setup';

describe('Runner end-to-end', () => {
  it('runs parallelize → map → reduce → eachPartition end-to-end', async () => {
    const store: Record<string, number> = {};

    const SearchCounter = defineJob({
      name: 'SearchCounter',
      job(): Job<string, number> {
        return new Job()
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
      },
    });

    await setupKraps({ jobs: [SearchCounter] });

    await createJob(SearchCounter).run();

    expect(store).toEqual({
      key1: 3, key2: 6, key3: 9, key4: 12, key5: 15,
      key6: 18, key7: 21, key8: 24, key9: 27,
    });
  });

  it('passes positional arguments through to job()', async () => {
    const store: Record<string, number> = {};

    const Counter = defineJob({
      name: 'Counter',
      job(multiplier: number): Job<string, number> {
        return new Job()
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
      },
    });

    await setupKraps({ jobs: [Counter] });

    await createJob(Counter).run(2);

    expect(store).toEqual({ key1: 2, key2: 4, key3: 6 });
  });

  it('supports dump and load', async () => {
    const collected: [number, [unknown, unknown][]][] = [];

    const partitioner: Partitioner<string> = hashPartitioner as Partitioner<string>;

    const DumpLoad = defineJob({
      name: 'DumpLoad',
      job(): [Job<string, number>, Job<string, number>] {
        const writeJob = new Job()
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 4 })
          .map(function* (key) {
            yield [key, parseInt(key.replace('key', ''), 10)];
          })
          .dump({ prefix: 'path/to/dump' });

        const readJob = new Job()
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
      },
    });

    await setupKraps({ jobs: [DumpLoad] });

    await createJob(DumpLoad).run();

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
    const store: Record<string, number> = {};

    const Counter = defineJob({
      name: 'Counter',
      job(): Job<string, number> {
        return new Job()
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
      },
    });

    await setupKraps({ jobs: [Counter] });

    await createJob(Counter).run();

    expect(store).toEqual({
      key1: 3, key2: 6, key3: 9, key4: 12, key5: 15,
      key6: 18, key7: 21, key8: 24, key9: 27,
    });
  });

  it('appends two jobs and yields the union of their pairs per partition', async () => {
    const store: [string, number][] = [];

    const Appender = defineJob({
      name: 'Appender',
      job(): Job<string, number> {
        const leftJob = new Job()
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3']) yield [item, 1] as [string, number];
          })
          .map(function* (key, value) {
            yield [key, value + 1];
          });

        const rightJob = new Job()
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
      },
    });

    await setupKraps({ jobs: [Appender] });

    await createJob(Appender).run();

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
    const store: Record<string, number> = {};

    const Combiner = defineJob({
      name: 'Combiner',
      job(): Job<string, number> {
        const job1 = new Job()
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3', 'key4', 'key5']) yield [item, 1] as [string, number];
          });

        const job2 = new Job()
          .parallelize(() => [1] as number[], { partitions: 8 })
          .map(function* () {
            for (const item of ['key1', 'key2', 'key3', 'key4']) yield [item, 2] as [string, number];
          })
          .combine<number, number>(job1, (key, leftValue, rightValue) =>
            [[key, leftValue + (rightValue ?? 0)]],
          );

        const job3 = new Job()
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
      },
    });

    await setupKraps({ jobs: [Combiner] });

    await createJob(Combiner).run();

    expect(store).toEqual({ key1: 6, key2: 6, key3: 6 });
  });

  it('does not execute shared steps more than once', async () => {
    let parallelizeCalls = 0;
    let mapCalls = 0;
    let reduceCalls = 0;

    const Shared = defineJob({
      name: 'Shared',
      job(): [Job<string, number>, Job<string, number>] {
        const reducedJob = new Job()
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
      },
    });

    await setupKraps({ jobs: [Shared] });

    await createJob(Shared).run();

    expect(parallelizeCalls).toBe(1);
    expect(mapCalls).toBe(1);
    expect(reduceCalls).toBe(1);
  });

  it('invokes the configured enqueuer with the right payloads', async () => {
    const enqueuer = vi.fn(async (json: string) => {
      const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });

      await worker.run({ retries: 0 });
    });

    const Pipeline = defineJob({
      name: 'Pipeline',
      job(): Job<string, number> {
        return new Job()
          .parallelize(() => ['item1', 'item2'], { partitions: 4 })
          .map(function* (key) {
            yield [key, 1] as [string, number];
          }, { jobs: 3 })
          .reduce((_key, leftValue, rightValue) => leftValue + rightValue, { jobs: 2 });
      },
    });

    await setupKraps({ enqueuer, jobs: [Pipeline] });

    await createJob(Pipeline).run();

    const calls = enqueuer.mock.calls.map(([payload]) => JSON.parse(payload as string));

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
    const enqueuer = vi.fn(async (json: string) => {
      const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });

      await worker.run({ retries: 0 });
    });

    const Capped = defineJob({
      name: 'Capped',
      job(): Job<string, number> {
        return new Job()
          .parallelize(() => ['item1', 'item2'], { partitions: 4 })
          .map(function* (key) {
            yield [key, 1] as [string, number];
          }, { jobs: 8 });
      },
    });

    await setupKraps({ enqueuer, jobs: [Capped] });

    await createJob(Capped).run();

    const mapCalls = enqueuer.mock.calls.filter(([payload]) => {
      const parsed = JSON.parse(payload as string);
      return parsed.stepIndex === 1;
    });

    expect(mapCalls).toHaveLength(4);
  });

  it('routes a step to its custom enqueuer instead of the configured default', async () => {
    const trackStep = (track: number[]) => async (json: string) => {
      track.push(JSON.parse(json).stepIndex);
      const worker = new Worker(json, { memoryLimit: 128 * 1024 * 1024, chunkLimit: 64, concurrency: 8 });
      await worker.run({ retries: 0 });
    };

    const defaultStepIndices: number[] = [];
    const customStepIndices: number[] = [];

    const defaultEnqueuer = trackStep(defaultStepIndices);
    const customEnqueuer = trackStep(customStepIndices);

    const Routed = defineJob({
      name: 'Routed',
      job(): Job<string, number> {
        return new Job()
          .parallelize(() => ['item1', 'item2'], { partitions: 4 })
          .map(function* (key) {
            yield [key, 1] as [string, number];
          }, { enqueuer: customEnqueuer })
          .reduce((_key, left, right) => left + right);
      },
    });

    await setupKraps({ enqueuer: defaultEnqueuer, jobs: [Routed] });

    await createJob(Routed).run();

    // parallelize is step 0, the custom-routed map is step 1, reduce is step 2.
    expect(new Set(customStepIndices)).toEqual(new Set([1]));
    expect(new Set(defaultStepIndices)).toEqual(new Set([0, 2]));
  });

  it('throws JobStopped when the redis queue is stopped before the runner finishes', async () => {
    const stoppedSpy = vi.spyOn(RedisQueue.prototype, 'stopped').mockResolvedValue(true);

    const Stoppable = defineJob({
      name: 'Stoppable',
      job(): Job<string, null> {
        return new Job()
          .parallelize(function* () {
            for (let index = 1; index <= 9; index++) yield `key${index}`;
          }, { partitions: 8 })
          .map<string, null>(() => []);
      },
    });

    await setupKraps({ jobs: [Stoppable] });

    await expect(createJob(Stoppable).run()).rejects.toThrow(JobStopped);

    stoppedSpy.mockRestore();
  });
});
