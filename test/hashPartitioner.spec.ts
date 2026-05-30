import { describe, it, expect } from 'vitest';
import { hashPartitioner } from '../src/hashPartitioner';

describe('hashPartitioner', () => {
  it('returns an integer in [0, numPartitions)', () => {
    for (let index = 0; index < 100; index++) {
      const partition = hashPartitioner(`key-${index}`, 16);

      expect(Number.isInteger(partition)).toBe(true);
      expect(partition).toBeGreaterThanOrEqual(0);
      expect(partition).toBeLessThan(16);
    }
  });

  it('is deterministic', () => {
    expect(hashPartitioner('key', 16)).toBe(hashPartitioner('key', 16));
  });

  it('partitions JSON-serialized keys', () => {
    expect(hashPartitioner(['a', 1], 8)).toBe(hashPartitioner(['a', 1], 8));
    expect(hashPartitioner([1, [2, 'x']], 8)).toBe(hashPartitioner([1, [2, 'x']], 8));
  });

  it('throws when numPartitions is 0 or negative', () => {
    expect(() => hashPartitioner('key', 0)).toThrow('numPartitions must be > 0');
    expect(() => hashPartitioner('key', -1)).toThrow('numPartitions must be > 0');
  });
});
