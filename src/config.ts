import { Redis } from 'ioredis';
import type { Driver } from './drivers/Driver';
import type { AnyJob } from './jobResolver';

export type Enqueuer = (worker: unknown, json: string) => void | Promise<void>;

export type JobClassRegistry = Record<
  string,
  new (...args: any[]) => { run(): AnyJob | AnyJob[] | Promise<AnyJob | AnyJob[]> }
>;

export type KrapsConfig = {
  driver: Driver,
  redis: Redis,
  namespace: string | null,
  jobTtl: number,
  showProgress: boolean,
  enqueuer: Enqueuer,
  jobClasses: JobClassRegistry,
};

export type ConfigureOptions = {
  driver: Driver,
  redis?: Redis,
  namespace?: string | null,
  jobTtl?: number,
  showProgress?: boolean,
  enqueuer?: Enqueuer,
  jobClasses?: JobClassRegistry,
};

const FOUR_DAYS_SECONDS = 4 * 24 * 60 * 60;

const defaultEnqueuer: Enqueuer = () => {
  throw new Error('Kraps: no enqueuer configured');
};

let config: KrapsConfig | null = null;

export function configure(options: ConfigureOptions): void {
  config = {
    driver: options.driver,
    redis: options.redis ?? new Redis(),
    namespace: options.namespace ?? null,
    jobTtl: options.jobTtl ?? FOUR_DAYS_SECONDS,
    showProgress: options.showProgress ?? true,
    enqueuer: options.enqueuer ?? defaultEnqueuer,
    jobClasses: options.jobClasses ?? {},
  };
}

export function getConfig(): KrapsConfig {
  if (!config) throw new Error('Kraps: not configured — call configure() first');

  return config;
}
