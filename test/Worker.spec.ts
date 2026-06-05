import { describe, it, expect } from 'vitest';
import { Redis } from 'ioredis';
import { Action } from '../src/actions';
import { FakeDriver } from '../src/drivers/FakeDriver';
import { hashPartitioner } from '../src/hashPartitioner';
import { Job } from '../src/Job';
import { RedisQueue } from '../src/RedisQueue';
import { Worker } from '../src/Worker';
import { gunzipPairLines, gzipPairLines } from './helpers/chunks';
import { setupKraps } from './helpers/setup';

const TOKEN = 'token';
const PREVIOUS_TOKEN = 'previous_token';

function buildWorker(args: Record<string, unknown>): Worker {
  return new Worker(JSON.stringify(args), {
    memoryLimit: 128 * 1024 * 1024,
    chunkLimit: 32,
    concurrency: 4,
  });
}

function buildQueue(redis: Redis): RedisQueue {
  return new RedisQueue({ redis, token: TOKEN, namespace: null, ttl: 60 });
}

function decode(driver: FakeDriver, name: string): [unknown, unknown][] {
  const buffer = driver.objects.get(name);

  if (!buffer) throw new Error(`No such object: ${name}`);

  return gunzipPairLines(buffer);
}

describe('Worker', () => {
  it('runs the before block when defined', async () => {
    let beforeCalled = false;

    class BeforeJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [], { partitions: 8, before: () => { beforeCalled = true; } });
      }
    }

    const { redis } = await setupKraps({ jobs: { BeforeJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'item1', part: '0' });

    await buildWorker({
      token: TOKEN,
      jobName: 'BeforeJob',
      args: [],
      jobIndex: 0,
      stepIndex: 0,
      frame: {},
    }).run({ retries: 0 });

    expect(beforeCalled).toBe(true);
  });

  it('parallelize stores one chunk per item under the partition assigned by the partitioner', async () => {
    class ParallelizeJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => ['item1', 'item2', 'item3'], { partitions: 8 });
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { ParallelizeJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'item1', part: '0' });

    await buildWorker({
      token: TOKEN,
      jobName: 'ParallelizeJob',
      args: [],
      jobIndex: 0,
      stepIndex: 0,
      frame: {},
    }).run({ retries: 0 });

    const expectedPartition = hashPartitioner('item1', 8);
    const objectName = `prefix/${TOKEN}/${expectedPartition}/chunk.0.json`;

    expect(await driver.list()).toEqual([objectName]);
    expect(decode(driver, objectName)).toEqual([['item1', null]]);
  });

  it('map reads previous-step chunks and writes mapped output partitioned by the new key', async () => {
    class MapJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [] as string[], { partitions: 4 })
          .map((key) => [[key, 1] as [string, number]])
          .map((key, value) => [[`${key}-extra`, value + 1] as [string, number]]);
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { MapJob } });
    const queue = buildQueue(redis);

    const chunk = gzipPairLines([['item1', 1], ['item2', 1]]);
    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, chunk);

    await queue.enqueue({ partition: 0 });

    await buildWorker({
      token: TOKEN,
      jobName: 'MapJob',
      args: [],
      jobIndex: 0,
      stepIndex: 2,
      frame: { token: PREVIOUS_TOKEN, partitions: 4 },
    }).run({ retries: 0 });

    const previousFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${PREVIOUS_TOKEN}/`));
    const outputFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${TOKEN}/`));

    expect(previousFiles).toEqual([`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`]);
    expect(outputFiles.length).toBeGreaterThan(0);

    const writtenPairs = outputFiles.flatMap((name) => decode(driver, name));

    expect(writtenPairs.sort((leftPair, rightPair) => (leftPair[0] as string).localeCompare(rightPair[0] as string))).toEqual([
      ['item1-extra', 2],
      ['item2-extra', 2],
    ]);
  });

  it('map pre-reduces when the subsequent step is a reduce', async () => {
    class MapReduceJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [] as string[], { partitions: 4 })
          .map((key) => [
            [`${key}a`, 1] as [string, number],
            [`${key}a`, 1] as [string, number],
            [`${key}b`, 1] as [string, number],
          ])
          .reduce((_key, leftValue, rightValue) => leftValue + rightValue);
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { MapReduceJob } });
    const queue = buildQueue(redis);

    const chunk = gzipPairLines([['item1', null], ['item2', null]]);
    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, chunk);

    await queue.enqueue({ partition: 0 });

    await buildWorker({
      token: TOKEN,
      jobName: 'MapReduceJob',
      args: [],
      jobIndex: 0,
      stepIndex: 1,
      frame: { token: PREVIOUS_TOKEN, partitions: 4 },
    }).run({ retries: 0 });

    const outputFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${TOKEN}/`));
    const allPairs = outputFiles.flatMap((name) => decode(driver, name));

    const counts: Record<string, number> = {};

    for (const [key, value] of allPairs) {
      counts[key as string] = (counts[key as string] ?? 0) + (value as number);
    }

    expect(counts).toEqual({ item1a: 2, item1b: 1, item2a: 2, item2b: 1 });
  });

  it('mapPartitions hands the partition and a sorted-merged iterable to the user block', async () => {
    const captured: [number, [unknown, unknown][]][] = [];

    class MapPartitionsJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [] as string[], { partitions: 4 })
          .map((key) => [[key, 1] as [string, number]])
          .mapPartitions(async function* (partition, pairs) {
            const list: [string, number][] = [];

            for await (const pair of pairs) list.push(pair);

            captured.push([partition, list]);

            for (const [key, value] of list) yield [`${key}x`, value + 1] as [string, number];
          });
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { MapPartitionsJob } });
    const queue = buildQueue(redis);

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, gzipPairLines([['item1', 1], ['item3', 1]]));
    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.1.json`, gzipPairLines([['item2', 1], ['item3', 1]]));

    await queue.enqueue({ partition: 0 });

    await buildWorker({
      token: TOKEN,
      jobName: 'MapPartitionsJob',
      args: [],
      jobIndex: 0,
      stepIndex: 2,
      frame: { token: PREVIOUS_TOKEN, partitions: 4 },
    }).run({ retries: 0 });

    expect(captured).toEqual([
      [0, [['item1', 1], ['item2', 1], ['item3', 1], ['item3', 1]]],
    ]);

    const outputFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${TOKEN}/`));
    const allPairs = outputFiles.flatMap((name) => decode(driver, name));

    expect(allPairs.sort((leftPair, rightPair) => (leftPair[0] as string).localeCompare(rightPair[0] as string))).toEqual([
      ['item1x', 2],
      ['item2x', 2],
      ['item3x', 2],
      ['item3x', 2],
    ]);
  });

  it('reduce merges the chunks for one partition and stores the reduced result', async () => {
    class ReduceJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [] as string[], { partitions: 4 })
          .map<string, number>(() => [])
          .reduce((_key, leftValue, rightValue) => leftValue + rightValue);
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { ReduceJob } });
    const queue = buildQueue(redis);

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, gzipPairLines([
      ['item1', 1],
      ['item1', 2],
      ['item2', 3],
      ['item3', 4],
    ]));

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.1.json`, gzipPairLines([
      ['item2', 1],
      ['item3', 2],
      ['item4', 2],
    ]));

    await queue.enqueue({ partition: 0 });

    await buildWorker({
      token: TOKEN,
      jobName: 'ReduceJob',
      args: [],
      jobIndex: 0,
      stepIndex: 2,
      frame: { token: PREVIOUS_TOKEN, partitions: 4 },
    }).run({ retries: 0 });

    const output = decode(driver, `prefix/${TOKEN}/0/chunk.0.json`);

    expect(output).toEqual([
      ['item1', 3],
      ['item2', 4],
      ['item3', 6],
      ['item4', 2],
    ]);
  });

  it('eachPartition feeds the user block with sorted, merged pairs', async () => {
    const captured: [number, [unknown, unknown][]][] = [];

    class EachPartitionJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [] as string[], { partitions: 4 })
          .eachPartition(async (partition, pairs) => {
            const list: [unknown, unknown][] = [];

            for await (const pair of pairs) list.push(pair);

            captured.push([partition, list]);
          });
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { EachPartitionJob } });
    const queue = buildQueue(redis);

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, gzipPairLines([['item1', 1], ['item2', 3]]));
    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.1.json`, gzipPairLines([['item2', 1], ['item3', 2]]));

    await queue.enqueue({ partition: 0 });

    await buildWorker({
      token: TOKEN,
      jobName: 'EachPartitionJob',
      args: [],
      jobIndex: 0,
      stepIndex: 1,
      frame: { token: PREVIOUS_TOKEN, partitions: 4 },
    }).run({ retries: 0 });

    expect(captured).toEqual([
      [0, [['item1', 1], ['item2', 1], ['item2', 3], ['item3', 2]]],
    ]);
  });

  it('append merges the chunks of two frames into one mapper output', async () => {
    class AppendJob {
      run(): Job<any, any>[] {
        const leftJob = new Job()
          .parallelize(() => [] as string[], { partitions: 2 })
          .map<string, number>(() => []);

        const rightJob = new Job()
          .parallelize(() => [] as string[], { partitions: 2 })
          .map<string, number>(() => []);

        return [leftJob, rightJob, rightJob.append(leftJob)];
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { AppendJob } });
    const queue = buildQueue(redis);

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, gzipPairLines([['key1', 1], ['key2', 2]]));
    await driver.store('prefix/append_token/0/chunk.0.json', gzipPairLines([['keyA', 10], ['keyB', 20]]));

    await queue.enqueue({ partition: 0, appendFrame: { token: 'append_token', partitions: 2 } });

    await buildWorker({
      token: TOKEN,
      jobName: 'AppendJob',
      args: [],
      jobIndex: 2,
      stepIndex: 2,
      frame: { token: PREVIOUS_TOKEN, partitions: 2 },
    }).run({ retries: 0 });

    const outputFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${TOKEN}/`));
    const allPairs = outputFiles.flatMap((name) => decode(driver, name));

    expect(allPairs.sort((leftPair, rightPair) => (leftPair[0] as string).localeCompare(rightPair[0] as string))).toEqual([
      ['key1', 1],
      ['key2', 2],
      ['keyA', 10],
      ['keyB', 20],
    ]);
  });

  it('combine joins matching keys and omits keys missing on the joined side', async () => {
    class CombineJob {
      run(): Job<any, any>[] {
        const otherJob = new Job()
          .parallelize(() => [] as string[], { partitions: 2 })
          .map<string, number>(() => [])
          .reduce((_key, leftValue, rightValue) => leftValue + rightValue);

        const job = new Job()
          .parallelize(() => [] as string[], { partitions: 2 })
          .map<string, number>(() => [])
          .combine<number, [number, number | null]>(otherJob, (key, leftValue, rightValue) =>
            [[key, [leftValue, rightValue] as [number, number | null]]],
          );

        return [otherJob, job];
      }
    }

    const { driver, redis } = await setupKraps({ jobs: { CombineJob } });
    const queue = buildQueue(redis);

    await driver.store(`prefix/${PREVIOUS_TOKEN}/0/chunk.0.json`, gzipPairLines([
      ['shared', 1],
      ['onlyLeft', 2],
    ]));

    await driver.store('prefix/combine_token/0/chunk.0.json', gzipPairLines([
      ['shared', 10],
      ['onlyRight', 99],
    ]));

    await queue.enqueue({ partition: 0, combineFrame: { token: 'combine_token', partitions: 2 } });

    await buildWorker({
      token: TOKEN,
      jobName: 'CombineJob',
      args: [],
      jobIndex: 1,
      stepIndex: 2,
      frame: { token: PREVIOUS_TOKEN, partitions: 2 },
    }).run({ retries: 0 });

    const outputFiles = (await driver.list()).filter((name) => name.startsWith(`prefix/${TOKEN}/`));
    const allPairs = outputFiles.flatMap((name) => decode(driver, name));

    const sorted = allPairs.sort((leftPair, rightPair) => (leftPair[0] as string).localeCompare(rightPair[0] as string));

    expect(sorted).toEqual([
      ['onlyLeft', [2, null]],
      ['shared', [1, 10]],
    ]);
  });

  it('does not run when the redis queue is already stopped', async () => {
    let beforeCalled = false;

    class StoppedJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [], { partitions: 4, before: () => { beforeCalled = true; } });
      }
    }

    const { redis } = await setupKraps({ jobs: { StoppedJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'item1', part: '0' });
    await queue.stop();

    await buildWorker({
      token: TOKEN,
      jobName: 'StoppedJob',
      args: [],
      jobIndex: 0,
      stepIndex: 0,
      frame: {},
    }).run({ retries: 0 });

    expect(beforeCalled).toBe(false);
  });

  it('aborts the run loop when the signal is aborted mid-run', async () => {
    const controller = new AbortController();
    const seen: string[] = [];

    class AbortJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [], { partitions: 4, before: () => { seen.push('before'); controller.abort(); } });
      }
    }

    const { redis } = await setupKraps({ jobs: { AbortJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'a', part: '0' });
    await queue.enqueue({ item: 'b', part: '1' });

    const worker = buildWorker({
      token: TOKEN,
      jobName: 'AbortJob',
      args: [],
      jobIndex: 0,
      stepIndex: 0,
      frame: {},
    });

    await expect(worker.run({ retries: 0, signal: controller.signal })).rejects.toThrow();

    expect(seen.length).toBeLessThan(2);
    expect(await queue.stopped()).toBe(false);
  });

  it('throws immediately when the signal is already aborted', async () => {
    let beforeCalled = false;

    class AbortBeforeRunJob {
      run(): Job<any, any> {
        return new Job()
          .parallelize(() => [], { partitions: 4, before: () => { beforeCalled = true; } });
      }
    }

    const { redis } = await setupKraps({ jobs: { AbortBeforeRunJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'a', part: '0' });

    const worker = buildWorker({
      token: TOKEN,
      jobName: 'AbortBeforeRunJob',
      args: [],
      jobIndex: 0,
      stepIndex: 0,
      frame: {},
    });

    const controller = new AbortController();
    controller.abort();

    await expect(worker.run({ retries: 0, signal: controller.signal })).rejects.toThrow();

    expect(beforeCalled).toBe(false);
  });

  it('rejects an unknown action', async () => {
    class BadJob {
      run(): Job<any, any> {
        const job = new Job().parallelize(() => [], { partitions: 4 });

        job.steps[0].action = 'totally_unknown' as Action;

        return job;
      }
    }

    const { redis } = await setupKraps({ jobs: { BadJob } });
    const queue = buildQueue(redis);

    await queue.enqueue({ item: 'x', part: '0' });

    await expect(
      buildWorker({
        token: TOKEN,
        jobName: 'BadJob',
        args: [],
        jobIndex: 0,
        stepIndex: 0,
        frame: {},
      }).run({ retries: 0 }),
    ).rejects.toThrow('Invalid action');
  });
});
