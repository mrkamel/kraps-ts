import { Action } from './actions';
import { Enqueuer } from './config';
import { Frame } from './Frame';
import { Partitioner } from './hashPartitioner';

export type StepBlock = (...args: any[]) => unknown | Promise<unknown>;

export interface JobLike {
  readonly steps: Step[];
}

export type Step = {
  action: Action,
  partitioner: Partitioner,
  partitions: number,
  jobs?: number | null,
  block?: StepBlock,
  enqueuer?: Enqueuer,
  before?: (() => void | Promise<void>) | null,
  frame?: Frame,
  dependency?: JobLike,
  options?: Record<string, unknown>,
};
