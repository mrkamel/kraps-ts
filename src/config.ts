import { Redis } from 'ioredis';
import type { Driver } from './drivers/Driver';
import type { KrapsJobClass } from './KrapsJob';

export type Enqueuer = (json: string) => void | Promise<void>;
export type JobClassRegistry = KrapsJobClass[];

export type KrapsConfig = {
  driver: Driver,
  redis: Redis,
  namespace: string | null,
  jobTtl: number,
  showProgress: boolean,
  enqueuer: Enqueuer,
  jobClasses: JobClassRegistry,
  jobClassByName: Map<string, KrapsJobClass>,
};

export type ConfigureOptions = {
  driver: Driver,
  redis?: Redis,
  namespace?: string | null,
  jobTtl?: number,
  showProgress?: boolean,
  enqueuer: Enqueuer,
  jobClasses?: JobClassRegistry,
};

const FOUR_DAYS_SECONDS = 4 * 24 * 60 * 60;

let config: KrapsConfig | null = null;

export function configure(options: ConfigureOptions): void {
  const jobClasses = options.jobClasses ?? [];
  const jobClassByName = buildJobClassIndex(jobClasses);

  config = {
    driver: options.driver,
    redis: options.redis ?? new Redis(),
    namespace: options.namespace ?? null,
    jobTtl: options.jobTtl ?? FOUR_DAYS_SECONDS,
    showProgress: options.showProgress ?? true,
    enqueuer: options.enqueuer,
    jobClasses,
    jobClassByName,
  };
}

export function getConfig(): KrapsConfig {
  if (!config) throw new Error('Kraps: not configured — call configure() first');

  return config;
}

export function findJobClass(name: string): KrapsJobClass | undefined {
  return getConfig().jobClassByName.get(name);
}

function buildJobClassIndex(jobClasses: JobClassRegistry): Map<string, KrapsJobClass> {
  const index = new Map<string, KrapsJobClass>();

  for (const klass of jobClasses) {
    if (typeof klass.jobName !== 'string') {
      throw new Error(`Kraps: job class ${klass.name || '<anonymous>'} is missing a static jobName string`);
    }

    if (klass.jobName.length === 0) {
      throw new Error(`Kraps: job class ${klass.name || '<anonymous>'}.jobName must be a non-empty string`);
    }

    if (index.has(klass.jobName)) {
      throw new Error(`Kraps: duplicate jobName "${klass.jobName}" in jobClasses`);
    }

    index.set(klass.jobName, klass);
  }

  return index;
}
