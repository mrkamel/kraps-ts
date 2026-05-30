import { describe, it, expect } from 'vitest';
import { Actions } from '../src/actions';
import { hashPartitioner, Partitioner } from '../src/hashPartitioner';
import { Job } from '../src/Job';

const WORKER_A = 'WorkerA';
const WORKER_B = 'WorkerB';

describe('Job', () => {
  describe('parallelize', () => {
    it('adds a parallelize step with defaults', () => {
      const block = () => [];

      const job = new Job({ worker: WORKER_A }).parallelize(block, { partitions: 8 });

      expect(job.steps).toHaveLength(1);

      expect(job.steps[0]).toMatchObject({
        action: Actions.PARALLELIZE,
        partitions: 8,
        partitioner: hashPartitioner,
        worker: WORKER_A,
        before: null,
        block,
      });
    });

    it('respects the passed partitioner, worker and before', () => {
      const block = () => [];
      const partitioner: Partitioner = (key) => key as number;
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(block, { partitions: 16, partitioner, worker: WORKER_B, before });

      expect(job.steps[0]).toMatchObject({
        action: Actions.PARALLELIZE,
        partitions: 16,
        partitioner,
        worker: WORKER_B,
        before,
        block,
      });
    });
  });

  describe('map', () => {
    it('inherits partitions from the previous step and defaults the worker', () => {
      const block = () => [];

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 4 })
        .map(block, { partitions: 8 });

      expect(job.steps).toHaveLength(2);

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP,
        partitions: 8,
        jobs: 4,
        worker: WORKER_A,
        before: null,
        block,
      });
    });

    it('respects the passed jobs, partitions, partitioner, worker and before', () => {
      const block = () => [];
      const partitioner: Partitioner = (key) => key as number;
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(block, { jobs: 4, partitions: 16, partitioner, worker: WORKER_B, before });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP,
        jobs: 4,
        partitions: 16,
        partitioner,
        worker: WORKER_B,
        before,
        block,
      });
    });
  });

  describe('mapPartitions', () => {
    it('adds a mapPartitions step', () => {
      const block = () => [];

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 4 })
        .mapPartitions(block, { partitions: 8 });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP_PARTITIONS,
        partitions: 8,
        jobs: 4,
        worker: WORKER_A,
        before: null,
        block,
      });
    });

    it('respects the passed options', () => {
      const block = () => [];
      const partitioner: Partitioner = (key) => key as number;
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .mapPartitions(block, { jobs: 4, partitions: 16, partitioner, worker: WORKER_B, before });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP_PARTITIONS,
        jobs: 4,
        partitions: 16,
        partitioner,
        worker: WORKER_B,
        before,
        block,
      });
    });
  });

  describe('reduce', () => {
    it('adds a reduce step with defaults inherited from the previous step', () => {
      const block = (_key: unknown, _left: unknown, _right: unknown) => 0;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => [])
        .reduce(block);

      expect(job.steps[2]).toMatchObject({
        action: Actions.REDUCE,
        partitions: 8,
        partitioner: hashPartitioner,
        worker: WORKER_A,
        before: null,
        block,
      });
    });

    it('respects the passed jobs, worker and before', () => {
      const block = (_key: unknown, _left: unknown, _right: unknown) => 0;
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => [])
        .reduce(block, { jobs: 4, before, worker: WORKER_B });

      expect(job.steps[2]).toMatchObject({
        action: Actions.REDUCE,
        jobs: 4,
        partitions: 8,
        worker: WORKER_B,
        before,
        block,
      });
    });
  });

  describe('append', () => {
    it('adds an append step with the dependency and step index', () => {
      const job1 = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => []);

      const job2 = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => [])
        .append(job1);

      expect(job2.steps[2]).toMatchObject({
        action: Actions.APPEND,
        partitions: 8,
        worker: WORKER_A,
        before: null,
        dependency: job1,
        options: { appendStepIndex: 1 },
      });
    });

    it('respects the passed jobs, worker and before', () => {
      const before = () => undefined;

      const job1 = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => []);

      const job2 = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .map(() => [])
        .append(job1, { jobs: 4, worker: WORKER_B, before });

      expect(job2.steps[2]).toMatchObject({
        action: Actions.APPEND,
        jobs: 4,
        partitions: 8,
        worker: WORKER_B,
        before,
      });
    });
  });

  describe('combine', () => {
    it('adds a combine step with the dependency and step index', () => {
      const block = () => [] as [string, number][];

      const job1 = new Job({ worker: WORKER_A })
        .parallelize<string>(() => [], { partitions: 8 })
        .map<string, number>(() => []);

      const job2 = new Job({ worker: WORKER_A })
        .parallelize<string>(() => [], { partitions: 8 })
        .map<string, number>(() => [])
        .combine(job1, block);

      expect(job2.steps[2]).toMatchObject({
        action: Actions.COMBINE,
        partitions: 8,
        worker: WORKER_A,
        before: null,
        block,
        dependency: job1,
        options: { combineStepIndex: 1 },
      });
    });

    it('respects the passed jobs, worker and before', () => {
      const block = () => [] as [string, number][];
      const before = () => undefined;

      const job1 = new Job({ worker: WORKER_A })
        .parallelize<string>(() => [], { partitions: 8 })
        .map<string, number>(() => []);

      const job2 = new Job({ worker: WORKER_A })
        .parallelize<string>(() => [], { partitions: 8 })
        .map<string, number>(() => [])
        .combine(job1, block, { jobs: 4, worker: WORKER_B, before });

      expect(job2.steps[2]).toMatchObject({
        action: Actions.COMBINE,
        jobs: 4,
        partitions: 8,
        worker: WORKER_B,
        before,
        block,
      });
    });
  });

  describe('repartition', () => {
    it('adds a map step with a passthrough block', () => {
      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .repartition({ partitions: 16 });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP,
        partitions: 16,
        worker: WORKER_A,
        before: null,
      });

      const collected = job.steps[1].block!('key', 'value');

      expect(collected).toEqual([['key', 'value']]);
    });

    it('respects the passed jobs, partitioner, worker and before', () => {
      const partitioner: Partitioner = (key) => key as number;
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .repartition({ jobs: 4, partitions: 16, partitioner, worker: WORKER_B, before });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP,
        jobs: 4,
        partitions: 16,
        partitioner,
        worker: WORKER_B,
        before,
      });
    });
  });

  describe('eachPartition', () => {
    it('adds an each_partition step with defaults inherited', () => {
      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .eachPartition(() => undefined);

      expect(job.steps[1]).toMatchObject({
        action: Actions.EACH_PARTITION,
        partitions: 8,
        worker: WORKER_A,
        before: null,
      });
    });

    it('respects the passed jobs, worker and before', () => {
      const before = () => undefined;

      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .eachPartition(() => undefined, { jobs: 4, worker: WORKER_B, before });

      expect(job.steps[1]).toMatchObject({
        action: Actions.EACH_PARTITION,
        jobs: 4,
        partitions: 8,
        worker: WORKER_B,
        before,
      });
    });
  });

  describe('dump', () => {
    it('adds an each_partition step', () => {
      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .dump({ prefix: 'path/to/destination' });

      expect(job.steps[1]).toMatchObject({
        action: Actions.EACH_PARTITION,
        partitions: 8,
        worker: WORKER_A,
        before: null,
      });
    });

    it('respects the passed worker', () => {
      const job = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 8 })
        .dump({ prefix: 'path/to/destination', worker: WORKER_B });

      expect(job.steps[1].worker).toBe(WORKER_B);
    });
  });

  describe('load', () => {
    it('adds a parallelize and a map_partitions step', () => {
      const partitioner: Partitioner = (key) => key as number;

      const job = new Job({ worker: WORKER_A })
        .load({ prefix: 'path/to/destination', partitions: 8, partitioner, concurrency: 8 });

      expect(job.steps).toHaveLength(2);

      expect(job.steps[0]).toMatchObject({
        action: Actions.PARALLELIZE,
        partitions: 8,
        worker: WORKER_A,
        before: null,
      });

      expect(job.steps[1]).toMatchObject({
        action: Actions.MAP_PARTITIONS,
        partitions: 8,
        partitioner,
        worker: WORKER_A,
        before: null,
      });
    });

    it('respects the passed worker', () => {
      const partitioner: Partitioner = (key) => key as number;

      const job = new Job({ worker: WORKER_A }).load({
        prefix: 'path/to/destination',
        partitions: 8,
        partitioner,
        concurrency: 8,
        worker: WORKER_B,
      });

      expect(job.steps[0].worker).toBe(WORKER_B);
      expect(job.steps[1].worker).toBe(WORKER_B);
    });
  });

  describe('immutability', () => {
    it('does not mutate the previous job when adding a step', () => {
      const previous = new Job({ worker: WORKER_A })
        .parallelize(() => [], { partitions: 4 });

      const next = previous.map(() => []);

      expect(previous.steps).toHaveLength(1);
      expect(next.steps).toHaveLength(2);
    });
  });
});
