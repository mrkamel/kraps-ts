import type { AnyJob } from './jobResolver';

export type KrapsJob<Args extends unknown[] = any[]> = {
  name: string;
  job(...args: Args): AnyJob | AnyJob[] | Promise<AnyJob | AnyJob[]>;
};

export function defineJob<Args extends unknown[]>(job: KrapsJob<Args>): KrapsJob<Args> {
  return job;
}
