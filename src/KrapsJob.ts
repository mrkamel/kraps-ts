import type { AnyJob } from './jobResolver';

export interface KrapsJob {
  run(): AnyJob | AnyJob[] | Promise<AnyJob | AnyJob[]>;
}

export interface KrapsJobClass<Args extends unknown[] = any[]> {
  new (...args: Args): KrapsJob;
  jobName: string;
}
