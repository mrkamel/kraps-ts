import { createReadStream } from 'fs';
import { Actions } from './actions';
import { findJobClass, getConfig } from './config';
import { downloadAll } from './downloader';
import { InvalidAction, InvalidJob, InvalidStep, KrapsError } from './errors';
import { Frame } from './Frame';
import { AnyJob, resolveJobs } from './jobResolver';
import { compare } from './mapReduce/compare';
import { GzippedLineWriter, readGzippedLines } from './mapReduce/lines';
import { Mapper } from './mapReduce/Mapper';
import { kWayMerge, Pair } from './mapReduce/merge';
import { Reducer as ReducerFn } from './mapReduce/reduce';
import { Reducer } from './mapReduce/Reducer';
import { parallelEach } from './parallelizer';
import { RedisQueue } from './RedisQueue';
import { Step } from './Step';
import { TempPath } from './TempPath';
import { TempPaths } from './TempPaths';
import { tryCatch } from './tryCatch';

type WorkerArgs = {
  jobName: string,
  args: unknown[],
  jobIndex: number,
  stepIndex: number,
  frame: Frame | Record<string, never>,
  token: string,
};

type DequeuedPayload = {
  part?: string,
  item?: unknown,
  partition?: number,
  combineFrame?: Frame,
  appendFrame?: Frame,
};

const RETRY_DELAY_MILLIS = 5_000;

export class Worker {
  private readonly args: WorkerArgs;
  private readonly memoryLimit: number;
  private readonly chunkLimit: number;
  private readonly concurrency: number;
  private readonly logger: { error: (...values: unknown[]) => void };

  private redisQueueCache: RedisQueue | null = null;
  private jobsCache: AnyJob[] | null = null;
  private stopRequested = false;
  private runPromise: Promise<void> | null = null;

  constructor(
    json: string,
    { memoryLimit, chunkLimit, concurrency, logger = { error: () => undefined } }:
    { memoryLimit: number, chunkLimit: number, concurrency: number, logger?: { error: (...values: unknown[]) => void } },
  ) {
    this.args = JSON.parse(json) as WorkerArgs;
    this.memoryLimit = memoryLimit;
    this.chunkLimit = chunkLimit;
    this.concurrency = concurrency;
    this.logger = logger;
  }

  async run({ retries = 3 }: { retries?: number } = {}): Promise<void> {
    if (this.runPromise) return this.runPromise;

    this.runPromise = this.runLoop(retries);

    return this.runPromise;
  }

  async stop(): Promise<void> {
    this.stopRequested = true;

    if (this.runPromise) await this.runPromise.catch(() => {});
  }

  private async runLoop(retries: number): Promise<void> {
    const redisQueue = this.redisQueue();

    if (this.stopRequested) return;
    if (await redisQueue.stopped()) return;

    await this.resolveJobsFromRegistry();

    const step = this.step();

    if (!Object.values(Actions).includes(step.action)) {
      throw new InvalidAction(`Invalid action ${step.action}`);
    }

    while (true) {
      if (this.stopRequested) break;
      if (await redisQueue.stopped()) break;
      if (await redisQueue.size() === 0) break;

      await redisQueue.dequeue(async (payload) => {
        if (payload === null) {
          await sleep(1_000);
          return;
        }

        await this.withRetries(retries, async () => {
          if (step.before) await step.before();
          await this.dispatch(step, payload as DequeuedPayload);
        });
      });
    }
  }

  private async dispatch(step: Step, payload: DequeuedPayload): Promise<void> {
    switch (step.action) {
      case Actions.PARALLELIZE:
        return this.performParallelize(payload);
      case Actions.MAP:
        return this.performMap(payload);
      case Actions.MAP_PARTITIONS:
        return this.performMapPartitions(payload);
      case Actions.REDUCE:
        return this.performReduce(payload);
      case Actions.COMBINE:
        return this.performCombine(payload);
      case Actions.APPEND:
        return this.performAppend(payload);
      case Actions.EACH_PARTITION:
        return this.performEachPartition(payload);
      default:
        throw new InvalidAction(`Invalid action ${step.action}`);
    }
  }

  private async performParallelize(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    await using mapper = new Mapper({
      implementation: {
        map: async (key: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
          await collector(key, null);
        },
      },
      partitioner: (key) => step.partitioner(key, step.partitions),
      memoryLimit: this.memoryLimit,
    });

    await mapper.map(payload.item);

    const partitions = await mapper.shuffle(this.chunkLimit);
    await this.uploadPartitions(partitions, String(payload.part));
  }

  private async performMap(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    const frame = this.frameOrThrow();
    await using tempPaths = await this.downloadAllForPartition(frame.token, payload.partition!);

    await using mapper = new Mapper({
      implementation: {
        map: async (key: unknown, value: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
          const pairs = step.block!(key, value) as Iterable<Pair> | AsyncIterable<Pair>;

          for await (const [newKey, newValue] of pairs) await collector(newKey, newValue);
        },
        reduce: this.nextStepReducer(),
      },
      partitioner: (key) => step.partitioner(key, step.partitions),
      memoryLimit: this.memoryLimit,
    });

    for (const tempPath of tempPaths) {
      for await (const line of readGzippedLines(tempPath.path)) {
        const [key, value] = JSON.parse(line);
        await mapper.map(key, value);
      }
    }

    const partitions = await mapper.shuffle(this.chunkLimit);
    await this.uploadPartitions(partitions, String(payload.partition));
  }

  private async performAppend(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    const frame = this.frameOrThrow();
    const otherFrame = payload.appendFrame!;
    await using tempPaths1 = await this.downloadAllForPartition(frame.token, payload.partition!);
    await using tempPaths2 = await this.downloadAllForPartition(otherFrame.token, payload.partition!);

    await using mapper = new Mapper({
      implementation: {
        map: async (key: unknown, value: unknown, collector: (key: unknown, value?: unknown) => Promise<void>) => {
          await collector(key, value);
        },
        reduce: this.nextStepReducer(),
      },
      partitioner: (key) => step.partitioner(key, step.partitions),
      memoryLimit: this.memoryLimit,
    });

    for (const tempPaths of [tempPaths1, tempPaths2]) {
      for (const tempPath of tempPaths) {
        for await (const line of readGzippedLines(tempPath.path)) {
          const [key, value] = JSON.parse(line);
          await mapper.map(key, value);
        }
      }
    }

    const partitions = await mapper.shuffle(this.chunkLimit);
    await this.uploadPartitions(partitions, String(payload.partition));
  }

  private async performMapPartitions(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    const frame = this.frameOrThrow();
    await using tempPaths = await this.downloadAllForPartition(frame.token, payload.partition!);

    await using mapper = new Mapper({
      implementation: {
        map: async (pairs: AsyncIterable<Pair>, collector: (key: unknown, value?: unknown) => Promise<void>) => {
          const emitted = step.block!(payload.partition!, pairs) as Iterable<Pair> | AsyncIterable<Pair>;

          for await (const [newKey, newValue] of emitted) await collector(newKey, newValue);
        },
        reduce: this.nextStepReducer(),
      },
      partitioner: (key) => step.partitioner(key, step.partitions),
      memoryLimit: this.memoryLimit,
    });

    const merged = kWayMerge(tempPaths.toArray(), this.chunkLimit);
    await mapper.map(merged);

    const partitions = await mapper.shuffle(this.chunkLimit);
    await this.uploadPartitions(partitions, String(payload.partition));
  }

  private async performReduce(payload: DequeuedPayload): Promise<void> {
    const step = this.step();

    await using reducer = new Reducer({
      implementation: {
        reduce: ((key, leftValue, rightValue) => step.block!(key, leftValue, rightValue)) as ReducerFn,
      },
    });

    const driver = getConfig().driver;
    const files = await driver.list({ prefix: driver.withPrefix(`${this.frameOrThrow().token}/${payload.partition}/`) });

    await parallelEach(files, this.concurrency, async (file) => {
      const chunkPath = await reducer.addChunk();
      await driver.download(file, chunkPath);
    });

    await using output = await TempPath.create();
    await using writer = new GzippedLineWriter(output.path);

    for await (const pair of reducer.reduce(this.chunkLimit)) {
      await writer.write(JSON.stringify(pair));
    }

    await writer.close();

    await driver.store(
      driver.withPrefix(`${this.args.token}/${payload.partition}/chunk.${payload.partition}.json`),
      createReadStream(output.path),
    );
  }

  private async performCombine(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    const frame = this.frameOrThrow();
    const otherFrame = payload.combineFrame!;
    await using tempPaths1 = await this.downloadAllForPartition(frame.token, payload.partition!);
    await using tempPaths2 = await this.downloadAllForPartition(otherFrame.token, payload.partition!);

    const left = kWayMerge(tempPaths1.toArray(), this.chunkLimit);
    const right = kWayMerge(tempPaths2.toArray(), this.chunkLimit);

    await using mapper = new Mapper({
      implementation: {
        map: async (collector: (key: unknown, value?: unknown) => Promise<void>) => {
          for await (const [key, leftValue, rightValue] of combineStreams(left, right)) {
            const emitted = step.block!(key, leftValue, rightValue) as Iterable<Pair> | AsyncIterable<Pair>;

            for await (const [newKey, newValue] of emitted) await collector(newKey, newValue);
          }
        },
      },
      partitioner: (key) => step.partitioner(key, step.partitions),
      memoryLimit: this.memoryLimit,
    });

    await mapper.map();

    const partitions = await mapper.shuffle(this.chunkLimit);
    await this.uploadPartitions(partitions, String(payload.partition));
  }

  private async performEachPartition(payload: DequeuedPayload): Promise<void> {
    const step = this.step();
    const frame = this.frameOrThrow();
    const driver = getConfig().driver;
    const files = (await driver.list({ prefix: driver.withPrefix(`${frame.token}/${payload.partition}/`) })).sort();

    await using tempPaths = new TempPaths();
    const indexByFile = new Map<string, TempPath>();

    for (const file of files) {
      indexByFile.set(file, await tempPaths.add());
    }

    await parallelEach(files, this.concurrency, async (file) => {
      const tempPath = indexByFile.get(file);
      if (!tempPath) return;

      await driver.download(file, tempPath.path);
    });

    const merged = kWayMerge(tempPaths.toArray(), this.chunkLimit);
    await step.block!(payload.partition!, merged);
  }

  private async uploadPartitions(partitions: Map<number, TempPath>, suffix: string): Promise<void> {
    const driver = getConfig().driver;
    const entries = Array.from(partitions.entries());

    // Partial-failure note: object keys are deterministic (token / partition /
    // chunk.<suffix>.json), so a retried Worker.call re-uploads under the
    // same keys — idempotent. We do not delete remote objects on failure;
    // the storage lifecycle policy reclaims them.

    try {
      await parallelEach(entries, this.concurrency, async ([partition, tempPath]) => {
        await driver.store(
          driver.withPrefix(`${this.args.token}/${partition}/chunk.${suffix}.json`),
          createReadStream(tempPath.path),
        );
      });
    } finally {
      await Promise.all(entries.map(([, tempPath]) => tempPath.delete()));
    }
  }

  private async downloadAllForPartition(token: string, partition: number): Promise<TempPaths> {
    const driver = getConfig().driver;
    return downloadAll({ prefix: driver.withPrefix(`${token}/${partition}/`), concurrency: this.concurrency });
  }

  private nextStepReducer(): ReducerFn | undefined {
    const next = this.nextStep();
    if (!next || next.action !== Actions.REDUCE) return undefined;

    return ((key, leftValue, rightValue) => next.block!(key, leftValue, rightValue)) as ReducerFn;
  }

  private async withRetries(maxRetries: number, attempt: () => Promise<void>): Promise<void> {
    let attemptCount = 0;

    while (true) {
      const [error] = await tryCatch(attempt);
      if (!error) return;

      if (error instanceof KrapsError) {
        await this.redisQueue().stop();
        throw error;
      }

      if (attemptCount >= maxRetries) {
        await this.redisQueue().stop();
        throw error;
      }

      this.logger.error(error);
      attemptCount++;
      await sleep(RETRY_DELAY_MILLIS);
    }
  }

  private redisQueue(): RedisQueue {
    if (!this.redisQueueCache) {
      const krapsConfig = getConfig();

      this.redisQueueCache = new RedisQueue({
        redis: krapsConfig.redis,
        token: this.args.token,
        namespace: krapsConfig.namespace,
        ttl: krapsConfig.jobTtl,
      });
    }

    return this.redisQueueCache;
  }

  private async resolveJobsFromRegistry(): Promise<void> {
    if (this.jobsCache) return;

    const JobClass = findJobClass(this.args.jobName);
    if (!JobClass) throw new InvalidJob(`Unknown job ${this.args.jobName}; did you register it?`);

    const instance = new JobClass(...this.args.args);
    const result = await instance.run();

    this.jobsCache = resolveJobs(result as AnyJob | AnyJob[]);
  }

  private job(): AnyJob {
    if (!this.jobsCache) throw new Error('Worker.job() called before job resolution');

    const job = this.jobsCache[this.args.jobIndex];
    if (!job) throw new InvalidJob(`Can't find job ${this.args.jobIndex}`);

    return job;
  }

  private step(): Step {
    const step = this.job().steps[this.args.stepIndex];
    if (!step) throw new InvalidStep(`Can't find step ${this.args.stepIndex}`);

    return step;
  }

  private nextStep(): Step | null {
    return this.job().steps[this.args.stepIndex + 1] ?? null;
  }

  private frameOrThrow(): Frame {
    const frame = this.args.frame;
    if (!('token' in frame)) throw new Error('Frame is missing');

    return frame as Frame;
  }
}

async function* combineStreams(
  leftStream: AsyncIterable<Pair>,
  rightStream: AsyncIterable<Pair>,
): AsyncGenerator<[unknown, unknown, unknown]> {
  const leftIterator = leftStream[Symbol.asyncIterator]();
  const rightIterator = rightStream[Symbol.asyncIterator]();

  let leftCurrent = await advance(leftIterator);
  let rightCurrent = await advance(rightIterator);

  while (true) {
    if (leftCurrent === null) return;

    if (rightCurrent === null) {
      yield [leftCurrent[0], leftCurrent[1], null];
      leftCurrent = await advance(leftIterator);
      continue;
    }

    const order = compare(leftCurrent[0], rightCurrent[0]);

    if (order === 0) {
      while (leftCurrent !== null && compare(leftCurrent[0], rightCurrent[0]) === 0) {
        yield [leftCurrent[0], leftCurrent[1], rightCurrent[1]];
        leftCurrent = await advance(leftIterator);
      }

      rightCurrent = await advance(rightIterator);
    } else if (order < 0) {
      yield [leftCurrent[0], leftCurrent[1], null];
      leftCurrent = await advance(leftIterator);
    } else {
      rightCurrent = await advance(rightIterator);
    }
  }
}

async function advance(iterator: AsyncIterator<Pair>): Promise<Pair | null> {
  const next = await iterator.next();
  return next.done ? null : next.value;
}

function sleep(millis: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, millis));
}
