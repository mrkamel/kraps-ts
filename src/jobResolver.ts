import { JobLike } from './Step';

export type AnyJob = JobLike;

export function resolveJobs(jobs: AnyJob | AnyJob[]): AnyJob[] {
  const list = Array.isArray(jobs) ? jobs : [jobs];

  return uniqueByIdentity(flattenWithDependencies(list));
}

function flattenWithDependencies(jobs: AnyJob[]): AnyJob[] {
  return jobs.flatMap((job) => {
    const dependencies = job.steps
      .map((step) => step.dependency)
      .filter((dependency): dependency is AnyJob => dependency != null);

    return [...flattenWithDependencies(dependencies), job];
  });
}

function uniqueByIdentity(jobs: AnyJob[]): AnyJob[] {
  const seen = new Set<AnyJob>();
  const result: AnyJob[] = [];

  for (const job of jobs) {
    if (seen.has(job)) continue;

    seen.add(job);
    result.push(job);
  }

  return result;
}
