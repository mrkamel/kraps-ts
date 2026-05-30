import { describe, it, expect } from 'vitest';
import { PriorityQueue } from '../../src/mapReduce/PriorityQueue';

describe('PriorityQueue', () => {
  it('pops items in key order', () => {
    const queue = new PriorityQueue<string>();
    queue.push('c', 3);
    queue.push('a', 1);
    queue.push('b', 2);

    expect(queue.pop()).toBe('a');
    expect(queue.pop()).toBe('b');
    expect(queue.pop()).toBe('c');
    expect(queue.pop()).toBeUndefined();
  });

  it('preserves insertion order among equal keys', () => {
    const queue = new PriorityQueue<string>();
    queue.push('first', 'k');
    queue.push('second', 'k');
    queue.push('third', 'k');

    expect(queue.pop()).toBe('first');
    expect(queue.pop()).toBe('second');
    expect(queue.pop()).toBe('third');
  });

  it('supports array keys', () => {
    const queue = new PriorityQueue<string>();
    queue.push('mid', [0, 'b']);
    queue.push('first', [0, 'a']);
    queue.push('last', [1, 'a']);

    expect(queue.pop()).toBe('first');
    expect(queue.pop()).toBe('mid');
    expect(queue.pop()).toBe('last');
  });
});
