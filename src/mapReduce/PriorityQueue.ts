import { compare, KrapsKey } from './compare';

type Node<T> = {
  key: KrapsKey;
  sequence: number;
  value: T;
};

export class PriorityQueue<T> {
  private readonly heap: Node<T>[] = [];
  private sequence = 0;

  push(value: T, key: KrapsKey): void {
    this.heap.push({ key, sequence: this.sequence++, value });
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): T | undefined {
    if (this.heap.length === 0) return undefined;

    const top = this.heap[0];
    const last = this.heap.pop()!;

    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.sinkDown(0);
    }

    return top.value;
  }

  get size(): number {
    return this.heap.length;
  }

  private less(left: Node<T>, right: Node<T>): boolean {
    const order = compare(left.key, right.key);
    if (order !== 0) return order < 0;

    return left.sequence < right.sequence;
  }

  private bubbleUp(start: number): void {
    let index = start;

    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (!this.less(this.heap[index], this.heap[parent])) break;

      [this.heap[index], this.heap[parent]] = [this.heap[parent], this.heap[index]];
      index = parent;
    }
  }

  private sinkDown(start: number): void {
    let index = start;
    const length = this.heap.length;

    while (true) {
      const left = 2 * index + 1;
      const right = 2 * index + 2;
      let smallest = index;

      if (left < length && this.less(this.heap[left], this.heap[smallest])) smallest = left;
      if (right < length && this.less(this.heap[right], this.heap[smallest])) smallest = right;

      if (smallest === index) break;

      [this.heap[index], this.heap[smallest]] = [this.heap[smallest], this.heap[index]];
      index = smallest;
    }
  }
}
