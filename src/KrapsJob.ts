import type { AnyJob } from './jobResolver';

export interface KrapsJobClass<Args extends unknown[] = any[]> {
  new (...args: Args): { run(): AnyJob | AnyJob[] | Promise<AnyJob | AnyJob[]> };
}
