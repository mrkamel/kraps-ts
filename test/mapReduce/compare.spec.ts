import { describe, it, expect } from 'vitest';
import { compare } from '../../src/mapReduce/compare';

describe('compare', () => {
  it('returns 0 for equal scalars', () => {
    expect(compare(1, 1)).toBe(0);
    expect(compare('a', 'a')).toBe(0);
  });

  it('compares numbers numerically', () => {
    expect(compare(1, 2)).toBe(-1);
    expect(compare(2, 1)).toBe(1);
  });

  it('compares strings lexicographically', () => {
    expect(compare('a', 'b')).toBe(-1);
    expect(compare('b', 'a')).toBe(1);
  });

  it('compares arrays element-wise', () => {
    expect(compare([0, 'a'], [0, 'b'])).toBe(-1);
    expect(compare([1, 'a'], [0, 'b'])).toBe(1);
    expect(compare([0, 'a'], [0, 'a'])).toBe(0);
  });

  it('treats shorter arrays as less when prefixes are equal', () => {
    expect(compare([1, 2], [1, 2, 3])).toBe(-1);
    expect(compare([1, 2, 3], [1, 2])).toBe(1);
  });
});
