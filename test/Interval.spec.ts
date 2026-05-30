import { describe, it, expect } from 'vitest';
import { Interval } from '../src/Interval';

function sleep(millis: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, millis));
}

describe('Interval', () => {
  it('fires the handler at each tick', async () => {
    let fired = 0;
    const interval = new Interval(50, () => { fired += 1; });

    try {
      await sleep(180);

      expect(fired).toBeGreaterThanOrEqual(2);
      expect(fired).toBeLessThanOrEqual(4);
    } finally {
      await interval.stop();
    }
  });

  it('stops cleanly and waits for in-flight handlers', async () => {
    let inFlight = 0;
    let completed = 0;

    const interval = new Interval(20, async () => {
      inFlight++;
      await sleep(40);
      inFlight--;
      completed++;
    });

    await sleep(30);
    await interval.stop();

    expect(inFlight).toBe(0);
    expect(completed).toBeGreaterThanOrEqual(1);
  });

  it('does not fire after stop', async () => {
    let fired = 0;
    const interval = new Interval(20, () => { fired += 1; });

    await sleep(50);
    await interval.stop();

    const snapshot = fired;
    await sleep(50);

    expect(fired).toBe(snapshot);
  });
});
