import { describe, it, expect } from 'vitest';
import { parallelEach } from '../src/parallelizer';

describe('parallelEach', () => {
  it('processes every item exactly once', async () => {
    const items = [1, 2, 3, 4, 5];
    const seen: number[] = [];

    await parallelEach(items, 2, async (item) => {
      seen.push(item);
    });

    expect(seen.sort()).toEqual([1, 2, 3, 4, 5]);
  });

  it('runs handlers in parallel up to concurrency', async () => {
    let active = 0;
    let peak = 0;

    await parallelEach([1, 2, 3, 4, 5, 6, 7, 8], 3, async () => {
      active++;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 10));
      active--;
    });

    expect(peak).toBe(3);
  });

  it('throws the first error encountered', async () => {
    await expect(
      parallelEach([1, 2, 3], 2, async (item) => {
        if (item === 2) throw new Error(`boom ${item}`);
      }),
    ).rejects.toThrow('boom 2');
  });
});
