import { describe, it, expect } from 'vitest';
import { Job } from '../src/Job';
import { resolveJobs } from '../src/jobResolver';

describe('resolveJobs', () => {
  it('resolves the dependencies of steps correctly', () => {
    const job1 = new Job()
      .parallelize(() => [], { partitions: 8 });

    const job2 = new Job()
      .parallelize(() => [], { partitions: 8 })
      .combine(job1, () => []);

    const job3 = new Job()
      .parallelize(() => [], { partitions: 8 })
      .combine(job2, () => []);

    const job4 = new Job()
      .parallelize(() => [], { partitions: 8 })
      .combine(job1, () => []);

    const resolved = resolveJobs([job3, job4]);

    expect(resolved).toEqual([job1, job2, job3, job4]);
  });

  it('returns a single job in a list when no dependencies', () => {
    const job = new Job()
      .parallelize(() => [], { partitions: 4 });

    expect(resolveJobs(job)).toEqual([job]);
  });

  it('deduplicates jobs that appear multiple times', () => {
    const dependency = new Job()
      .parallelize(() => [], { partitions: 4 });

    const job1 = new Job()
      .parallelize(() => [], { partitions: 4 })
      .combine(dependency, () => []);

    const job2 = new Job()
      .parallelize(() => [], { partitions: 4 })
      .combine(dependency, () => []);

    const resolved = resolveJobs([job1, job2]);

    expect(resolved.filter((entry) => entry === dependency)).toHaveLength(1);
  });
});
