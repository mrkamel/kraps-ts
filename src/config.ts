import { Redis } from 'ioredis';
import type { Driver } from './drivers/Driver';
import type { KrapsJobClass } from './KrapsJob';

export type Enqueuer = (json: string) => void | Promise<void>;
export type JobClasses = Record<string, KrapsJobClass>;

export type KrapsConfig = {
  driver: Driver,
  redis: Redis,
  namespace: string | null,
  jobTtl: number,
  showProgress: boolean,
  enqueuer: Enqueuer,
  jobs: JobClasses,
  nameByClass: Map<KrapsJobClass, string>,
};

export type ConfigureOptions = {
  driver: Driver,
  redis?: Redis,
  namespace?: string | null,
  jobTtl?: number,
  showProgress?: boolean,
  enqueuer: Enqueuer,
  jobs?: JobClasses,
};

const FOUR_DAYS_SECONDS = 4 * 24 * 60 * 60;

let config: KrapsConfig | null = null;

export function configure(options: ConfigureOptions): void {
  const jobs = options.jobs ?? {};
  const nameByClass = buildNameByClass(jobs);

  config = {
    driver: options.driver,
    redis: options.redis ?? new Redis(),
    namespace: options.namespace ?? null,
    jobTtl: options.jobTtl ?? FOUR_DAYS_SECONDS,
    showProgress: options.showProgress ?? true,
    enqueuer: options.enqueuer,
    jobs,
    nameByClass,
  };
}

export function getConfig(): KrapsConfig {
  if (!config) throw new Error('Kraps: not configured — call configure() first');

  return config;
}

export function findJobClass(name: string): KrapsJobClass | undefined {
  return getConfig().jobs[name];
}

export function findJobName(klass: KrapsJobClass): string | undefined {
  return getConfig().nameByClass.get(klass);
}

function buildNameByClass(jobs: JobClasses): Map<KrapsJobClass, string> {
  const nameByClass = new Map<KrapsJobClass, string>();

  for (const [name, klass] of Object.entries(jobs)) {
    if (name.length === 0) {
      throw new Error('Kraps: job name must be a non-empty string');
    }

    const existing = nameByClass.get(klass);
    if (existing) {
      throw new Error(`Kraps: job class registered under two names: "${existing}" and "${name}"`);
    }

    nameByClass.set(klass, name);
  }

  return nameByClass;
}
