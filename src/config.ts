import { Redis } from 'ioredis';
import type { Driver } from './drivers/Driver';
import type { KrapsJob } from './KrapsJob';

export type Enqueuer = (json: string) => void | Promise<void>;
export type JobRegistry = KrapsJob[];

export type KrapsConfig = {
  driver: Driver,
  redis: Redis,
  namespace: string | null,
  jobTtl: number,
  showProgress: boolean,
  enqueuer: Enqueuer,
  jobs: JobRegistry,
  jobByName: Map<string, KrapsJob>,
};

export type ConfigureOptions = {
  driver: Driver,
  redis?: Redis,
  namespace?: string | null,
  jobTtl?: number,
  showProgress?: boolean,
  enqueuer: Enqueuer,
  jobs?: JobRegistry,
};

const FOUR_DAYS_SECONDS = 4 * 24 * 60 * 60;

let config: KrapsConfig | null = null;

export function configure(options: ConfigureOptions): void {
  const jobs = options.jobs ?? [];
  const jobByName = buildJobIndex(jobs);

  config = {
    driver: options.driver,
    redis: options.redis ?? new Redis(),
    namespace: options.namespace ?? null,
    jobTtl: options.jobTtl ?? FOUR_DAYS_SECONDS,
    showProgress: options.showProgress ?? true,
    enqueuer: options.enqueuer,
    jobs,
    jobByName,
  };
}

export function getConfig(): KrapsConfig {
  if (!config) throw new Error('Kraps: not configured — call configure() first');

  return config;
}

export function findJob(name: string): KrapsJob | undefined {
  return getConfig().jobByName.get(name);
}

function buildJobIndex(jobs: JobRegistry): Map<string, KrapsJob> {
  const index = new Map<string, KrapsJob>();

  for (const job of jobs) {
    if (index.has(job.name)) {
      throw new Error(`Kraps: duplicate job name "${job.name}" in jobs`);
    }

    index.set(job.name, job);
  }

  return index;
}
