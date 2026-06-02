import { Actions } from './actions';
import { downloadAll } from './downloader';
import { Enqueuer, getConfig } from './config';
import { Partitioner, hashPartitioner } from './hashPartitioner';
import { JsonValue, KrapsKey } from './mapReduce/compare';
import { readLines } from './mapReduce/lines';
import { Step, StepBlock } from './Step';

export type Emit<NewKey extends KrapsKey, NewValue extends JsonValue> =
  | Iterable<[NewKey, NewValue]>
  | AsyncIterable<[NewKey, NewValue]>;

export type Items<NewKey extends KrapsKey> = Iterable<NewKey> | AsyncIterable<NewKey>;
export type ParallelizeBlock<NewKey extends KrapsKey> = () => Items<NewKey>;

export type MapBlock<Key extends KrapsKey, Value extends JsonValue, NewKey extends KrapsKey, NewValue extends JsonValue> = (
  key: Key,
  value: Value,
) => Emit<NewKey, NewValue>;

export type MapPartitionsBlock<Key extends KrapsKey, Value extends JsonValue, NewKey extends KrapsKey, NewValue extends JsonValue> = (
  partition: number,
  pairs: AsyncIterable<[Key, Value]>,
) => Emit<NewKey, NewValue>;

export type ReduceBlock<Key extends KrapsKey, Value extends JsonValue> = (
  key: Key,
  leftValue: Value,
  rightValue: Value,
) => Value | Promise<Value>;

export type CombineBlock<Key extends KrapsKey, LeftValue extends JsonValue, RightValue extends JsonValue, ResultValue extends JsonValue> = (
  key: Key,
  leftValue: LeftValue,
  rightValue: RightValue | null,
) => Emit<Key, ResultValue>;

export type EachPartitionBlock<Key extends KrapsKey, Value extends JsonValue> = (
  partition: number,
  pairs: AsyncIterable<[Key, Value]>,
) => void | Promise<void>;

type BlockOptions = {
  enqueuer?: Enqueuer,
  before?: (() => void | Promise<void>) | null,
};

export class Job<Key extends KrapsKey = never, Value extends JsonValue = never> {
  readonly steps: Step[];
  private readonly enqueuer?: Enqueuer;
  private partitions: number;
  private partitioner: Partitioner;

  constructor({ enqueuer }: { enqueuer?: Enqueuer } = {}) {
    this.enqueuer = enqueuer;
    this.steps = [];
    this.partitions = 0;
    this.partitioner = hashPartitioner;
  }

  parallelize<NewKey extends KrapsKey>(
    block: ParallelizeBlock<NewKey>,
    options: { partitions: number, partitioner?: Partitioner<NewKey> } & BlockOptions,
  ): Job<NewKey, null> {
    const next = this.cloneFresh<NewKey, null>();
    next.partitions = options.partitions;
    next.partitioner = (options.partitioner ?? hashPartitioner) as Partitioner;

    next.steps.push({
      action: Actions.PARALLELIZE,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      block: block as StepBlock,
    });

    return next;
  }

  map<NewKey extends KrapsKey, NewValue extends JsonValue>(
    block: MapBlock<Key, Value, NewKey, NewValue>,
    options: { partitions?: number, partitioner?: Partitioner<NewKey>, jobs?: number } & BlockOptions = {},
  ): Job<NewKey, NewValue> {
    return this.appendBlockStep<NewKey, NewValue>(Actions.MAP, options, block as StepBlock);
  }

  mapPartitions<NewKey extends KrapsKey, NewValue extends JsonValue>(
    block: MapPartitionsBlock<Key, Value, NewKey, NewValue>,
    options: { partitions?: number, partitioner?: Partitioner<NewKey>, jobs?: number } & BlockOptions = {},
  ): Job<NewKey, NewValue> {
    return this.appendBlockStep<NewKey, NewValue>(Actions.MAP_PARTITIONS, options, block as StepBlock);
  }

  reduce(
    block: ReduceBlock<Key, Value>,
    options: { jobs?: number } & BlockOptions = {},
  ): Job<Key, Value> {
    const next = this.cloneFresh<Key, Value>();
    const cappedJobs = capJobs(options.jobs, next.partitions);

    next.steps.push({
      action: Actions.REDUCE,
      jobs: cappedJobs,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      block: block as StepBlock,
    });

    return next;
  }

  combine<RightValue extends JsonValue, ResultValue extends JsonValue>(
    other: Job<Key, RightValue>,
    block: CombineBlock<Key, Value, RightValue, ResultValue>,
    options: { jobs?: number } & BlockOptions = {},
  ): Job<Key, ResultValue> {
    const next = this.cloneFresh<Key, ResultValue>();
    const cappedJobs = capJobs(options.jobs, next.partitions);

    next.steps.push({
      action: Actions.COMBINE,
      jobs: cappedJobs,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      block: block as StepBlock,
      dependency: other,
      options: { combineStepIndex: other.steps.length - 1 },
    });

    return next;
  }

  append(other: Job<Key, Value>, options: { jobs?: number } & BlockOptions = {}): Job<Key, Value> {
    const next = this.cloneFresh<Key, Value>();
    const cappedJobs = capJobs(options.jobs, next.partitions);

    next.steps.push({
      action: Actions.APPEND,
      jobs: cappedJobs,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      dependency: other,
      options: { appendStepIndex: other.steps.length - 1 },
    });

    return next;
  }

  eachPartition(
    block: EachPartitionBlock<Key, Value>,
    options: { jobs?: number } & BlockOptions = {},
  ): Job<Key, Value> {
    const next = this.cloneFresh<Key, Value>();
    const cappedJobs = capJobs(options.jobs, next.partitions);

    next.steps.push({
      action: Actions.EACH_PARTITION,
      jobs: cappedJobs,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      block: block as StepBlock,
    });

    return next;
  }

  repartition(
    options: { partitions: number, partitioner?: Partitioner<Key>, jobs?: number } & BlockOptions,
  ): Job<Key, Value> {
    return this.map<Key, Value>((key, value) => [[key, value] as [Key, Value]], options);
  }

  dump(options: { prefix: string, enqueuer?: Enqueuer }): Job<Key, Value> {
    const prefix = options.prefix;

    return this.eachPartition(async (partition, pairs) => {
      const lines: string[] = [];

      for await (const pair of pairs) {
        lines.push(JSON.stringify(pair));
      }

      const body = lines.length > 0 ? `${lines.join('\n')}\n` : '';

      await getConfig().driver.store(`${prefix}/${partition}/chunk.json`, body);
    }, { enqueuer: options.enqueuer ?? this.enqueuer });
  }

  load<NewKey extends KrapsKey, NewValue extends JsonValue>(options: {
    prefix: string,
    partitions: number,
    partitioner: Partitioner<NewKey>,
    concurrency: number,
    enqueuer?: Enqueuer,
  }): Job<NewKey, NewValue> {
    const enqueuer = options.enqueuer ?? this.enqueuer;
    const sourcePrefix = options.prefix;
    const concurrency = options.concurrency;

    const seeded = this.parallelize<number>(
      function* () {
        for (let partition = 0; partition < options.partitions; partition++) yield partition;
      },
      {
        partitions: options.partitions,
        partitioner: (key) => key,
        enqueuer,
      },
    );

    return seeded.mapPartitions<NewKey, NewValue>(
      async function* (partition, _pairs) {
        await using tempPaths = await downloadAll({
          prefix: `${sourcePrefix}/${partition}/`,
          concurrency,
        });

        for (const tempPath of tempPaths) {
          for await (const line of readLines(tempPath.path)) {
            yield JSON.parse(line) as [NewKey, NewValue];
          }
        }
      },
      { partitioner: options.partitioner, enqueuer },
    );
  }

  private appendBlockStep<NewKey extends KrapsKey, NewValue extends JsonValue>(
    action: typeof Actions[keyof typeof Actions],
    options: { partitions?: number, partitioner?: Partitioner<NewKey>, jobs?: number } & BlockOptions,
    block: StepBlock,
  ): Job<NewKey, NewValue> {
    const next = this.cloneFresh<NewKey, NewValue>();
    const cappedJobs = capJobs(options.jobs, next.partitions);

    if (options.partitions != null) next.partitions = options.partitions;
    if (options.partitioner != null) next.partitioner = options.partitioner as Partitioner;

    next.steps.push({
      action,
      jobs: cappedJobs,
      partitions: next.partitions,
      partitioner: next.partitioner,
      enqueuer: options.enqueuer ?? this.enqueuer,
      before: options.before ?? null,
      block,
    });

    return next;
  }

  private cloneFresh<NewKey extends KrapsKey, NewValue extends JsonValue>(): Job<NewKey, NewValue> {
    const clone = Object.create(Job.prototype) as Job<NewKey, NewValue>;
    Object.assign(clone, this);
    (clone as { steps: Step[] }).steps = [...this.steps];

    return clone;
  }
}

function capJobs(requested: number | undefined, partitions: number): number | null {
  if (partitions === 0) return requested ?? null;
  if (requested == null) return partitions;

  return Math.min(requested, partitions);
}
