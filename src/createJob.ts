import type { KrapsJob } from './KrapsJob';
import { Runner } from './Runner';

export type JobInvocation<Args extends unknown[]> = {
  run(...args: Args): Promise<void>;
};

export function createJob<Args extends unknown[]>(job: KrapsJob<Args>): JobInvocation<Args> {
  return new Runner(job);
}
