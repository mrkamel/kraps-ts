import { randomBytes } from 'crypto';
import { SingleBar, Presets } from 'cli-progress';
import { Actions } from './actions';
import { getConfig } from './config';
import { IncompatibleFrame, InvalidAction, JobStopped } from './errors';
import { Frame } from './Frame';
import { Interval } from './Interval';
import { AnyJob, resolveJobs } from './jobResolver';
import { KrapsJobClass } from './KrapsJob';
import { RedisQueue } from './RedisQueue';
import { Step } from './Step';

type RunnerPayload = {
  jobIndex: number,
  stepIndex: number,
  frame: Frame | Record<string, never>,
  token: string,
  klass: string,
  args: unknown[],
};

const POLL_INTERVAL_MILLIS = 1_000;
const PROGRESS_UPDATE_INTERVAL_MILLIS = 1_000;

export class Runner<Args extends unknown[] = unknown[]> {
  private readonly klass: KrapsJobClass<Args>;
  private readonly className: string;
  private totalJobs = 0;

  constructor(klass: KrapsJobClass<Args>) {
    this.klass = klass;
    this.className = klass.jobName;
  }

  async run(...args: Args): Promise<void> {
    const instance = new this.klass(...args);
    const result = await instance.run();
    const jobs = resolveJobs(result);

    this.totalJobs = jobs.length;

    for (let jobIndex = 0; jobIndex < jobs.length; jobIndex++) {
      const job = jobs[jobIndex];
      let frame: Frame | undefined;

      for (let stepIndex = 0; stepIndex < job.steps.length; stepIndex++) {
        const step = job.steps[stepIndex];

        if (step.frame) {
          frame = step.frame;
          continue;
        }

        frame = await this.performStep({ jobs, jobIndex, stepIndex, frame, args });
        step.frame = frame;
      }
    }
  }

  private async performStep(
    { jobs, jobIndex, stepIndex, frame, args }:
    { jobs: AnyJob[], jobIndex: number, stepIndex: number, frame: Frame | undefined, args: unknown[] }
  ): Promise<Frame> {
    const job = jobs[jobIndex];
    const step = job.steps[stepIndex];
    const totalSteps = job.steps.length;

    switch (step.action) {
      case Actions.PARALLELIZE:
        return this.performParallelize({ step, jobIndex, stepIndex, totalSteps, args });
      case Actions.MAP:
      case Actions.MAP_PARTITIONS:
      case Actions.REDUCE:
        return this.performByPartition({ step, jobIndex, stepIndex, totalSteps, frame: requireFrame(frame), args });
      case Actions.COMBINE:
        return this.performCombine({ step, jobIndex, stepIndex, totalSteps, frame: requireFrame(frame), args });
      case Actions.APPEND:
        return this.performAppend({ step, jobIndex, stepIndex, totalSteps, frame: requireFrame(frame), args });
      case Actions.EACH_PARTITION:
        await this.performByPartition({ step, jobIndex, stepIndex, totalSteps, frame: requireFrame(frame), args });
        return requireFrame(frame);
      default:
        throw new InvalidAction(`Invalid action ${step.action}`);
    }
  }

  private async performParallelize(
    { step, jobIndex, stepIndex, totalSteps, args }:
    { step: Step, jobIndex: number, stepIndex: number, totalSteps: number, args: unknown[] }
  ): Promise<Frame> {
    const items: { item: unknown }[] = [];
    const iterable = step.block!() as Iterable<unknown> | AsyncIterable<unknown>;

    for await (const item of iterable) items.push({ item });

    const token = await this.pushAndWait({
      step,
      jobIndex,
      stepIndex,
      totalSteps,
      frame: undefined,
      args,
      payloads: items,
      jobCount: items.length,
    });

    return { token, partitions: step.partitions };
  }

  private async performByPartition(
    { step, jobIndex, stepIndex, totalSteps, frame, args }:
    { step: Step, jobIndex: number, stepIndex: number, totalSteps: number, frame: Frame, args: unknown[] }
  ): Promise<Frame> {
    const payloads = buildPartitionPayloads(frame.partitions);

    const token = await this.pushAndWait({
      step,
      jobIndex,
      stepIndex,
      totalSteps,
      frame,
      args,
      payloads,
      jobCount: step.jobs ?? payloads.length,
    });

    return { token, partitions: step.partitions };
  }

  private async performCombine(
    { step, jobIndex, stepIndex, totalSteps, frame, args }:
    { step: Step, jobIndex: number, stepIndex: number, totalSteps: number, frame: Frame, args: unknown[] }
  ): Promise<Frame> {
    const combineStep = combineDependencyStep(step, 'combineStepIndex');
    if (combineStep.partitions !== step.partitions) throw new IncompatibleFrame('Incompatible number of partitions');

    const combineFrame = requireFrame(combineStep.frame);

    const payloads = Array.from({ length: frame.partitions }, (_value, partition) => ({
      partition,
      combineFrame,
    }));

    const token = await this.pushAndWait({
      step,
      jobIndex,
      stepIndex,
      totalSteps,
      frame,
      args,
      payloads,
      jobCount: step.jobs ?? payloads.length,
    });

    return { token, partitions: step.partitions };
  }

  private async performAppend(
    { step, jobIndex, stepIndex, totalSteps, frame, args }:
    { step: Step, jobIndex: number, stepIndex: number, totalSteps: number, frame: Frame, args: unknown[] }
  ): Promise<Frame> {
    const appendStep = combineDependencyStep(step, 'appendStepIndex');
    if (appendStep.partitions !== step.partitions) throw new IncompatibleFrame('Incompatible number of partitions');

    const appendFrame = requireFrame(appendStep.frame);

    const payloads = Array.from({ length: frame.partitions }, (_value, partition) => ({
      partition,
      appendFrame,
    }));

    const token = await this.pushAndWait({
      step,
      jobIndex,
      stepIndex,
      totalSteps,
      frame,
      args,
      payloads,
      jobCount: step.jobs ?? payloads.length,
    });

    return { token, partitions: step.partitions };
  }

  private async pushAndWait({
    step,
    jobIndex,
    stepIndex,
    totalSteps,
    frame,
    args,
    payloads,
    jobCount,
  }: {
    step: Step,
    jobIndex: number,
    stepIndex: number,
    totalSteps: number,
    frame: Frame | undefined,
    args: unknown[],
    payloads: Record<string, unknown>[],
    jobCount: number,
  }): Promise<string> {
    const krapsConfig = getConfig();
    const token = randomBytes(16).toString('hex');

    const redisQueue = new RedisQueue({
      redis: krapsConfig.redis,
      token,
      namespace: krapsConfig.namespace,
      ttl: krapsConfig.jobTtl,
    });

    const progressBar = this.createProgressBar({
      jobIndex,
      stepIndex,
      totalSteps,
      total: payloads.length,
      step,
      token,
    });

    let interval: Interval | null = null;

    try {
      for (let part = 0; part < payloads.length; part++) {
        await redisQueue.enqueue({ ...payloads[part], part: String(part) });
      }

      if (progressBar) {
        interval = new Interval(PROGRESS_UPDATE_INTERVAL_MILLIS, async () => {
          const remaining = await redisQueue.size();
          const completed = Math.max(payloads.length - remaining, 0);

          progressBar.update(completed);
        });
      }

      const stopped = await redisQueue.stopped();

      if (!stopped) {
        const enqueuer = krapsConfig.enqueuer;

        const basePayload: RunnerPayload = {
          jobIndex,
          stepIndex,
          frame: frame ?? {},
          token,
          klass: this.className,
          args,
        };

        for (let index = 0; index < jobCount; index++) {
          if (await redisQueue.stopped()) break;

          await enqueuer(step.worker, JSON.stringify(basePayload));
        }
      }

      while (true) {
        if (await redisQueue.stopped()) break;

        const size = await redisQueue.size();
        if (size === 0) break;

        await sleep(POLL_INTERVAL_MILLIS);
      }

      if (await redisQueue.stopped()) throw new JobStopped('The job was stopped');

      if (progressBar) progressBar.update(payloads.length);

      return token;
    } catch (error) {
      await redisQueue.stop();
      throw error;
    } finally {
      if (interval) await interval.stop();
      if (progressBar) progressBar.stop();
    }
  }

  private createProgressBar({
    jobIndex,
    stepIndex,
    step,
    token,
    total,
    totalSteps,
  }: {
    jobIndex: number,
    stepIndex: number,
    step: Step,
    token: string,
    total: number,
    totalSteps: number,
  }): SingleBar | null {
    if (!getConfig().showProgress) return null;
    if (!process.stdout.isTTY) return null;

    const jobsLabel = step.jobs ?? '?';

    const format =
      `${this.className}: job ${jobIndex + 1}/${this.totalJobs}, step ${stepIndex + 1}/${totalSteps}, ` +
      `${jobsLabel} jobs, token ${token}, {duration_formatted}, {value}/{total} ({percentage}%) => ${step.action}`;

    const progressBar = new SingleBar({ format, hideCursor: true }, Presets.shades_classic);

    progressBar.start(Math.max(total, 1), 0);

    return progressBar;
  }
}

function sleep(millis: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, millis));
}

function buildPartitionPayloads(partitions: number): { partition: number }[] {
  return Array.from({ length: partitions }, (_value, partition) => ({ partition }));
}

function requireFrame(frame: Frame | undefined): Frame {
  if (!frame) throw new Error('Step requires a prior frame');

  return frame;
}

function combineDependencyStep(step: Step, optionKey: 'combineStepIndex' | 'appendStepIndex'): Step {
  if (!step.dependency || !step.options) throw new Error(`Step ${step.action} is missing dependency or options`);

  const index = step.options[optionKey] as number;
  const dependencyStep = step.dependency.steps[index];

  if (!dependencyStep) throw new Error(`Dependency step ${index} not found`);

  return dependencyStep;
}
