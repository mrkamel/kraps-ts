import type { AnyJob } from './jobResolver';

export type KrapsJob<Args extends unknown[] = unknown[]> = {
  name: string;
  job(...args: Args): AnyJob | AnyJob[] | Promise<AnyJob | AnyJob[]>;
};

export function defineJob<Args extends unknown[]>(job: KrapsJob<Args>): KrapsJob<Args> {
  if (typeof job.name !== 'string') {
    throw new Error('Kraps: defineJob requires a name string');
  }

  if (job.name.length === 0) {
    throw new Error('Kraps: defineJob name must be a non-empty string');
  }

  if (typeof job.job !== 'function') {
    throw new Error(`Kraps: defineJob "${job.name}" requires a job function`);
  }

  return job;
}
