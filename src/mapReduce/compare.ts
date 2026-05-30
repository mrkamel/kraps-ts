export type JsonValue = string | number | boolean | null | JsonValue[] | { [property: string]: JsonValue };
export type KrapsKey = string | number | boolean | null | KrapsKey[];

export function compare(left: KrapsKey, right: KrapsKey): number {
  if (left === null && right === null) return 0;
  if (left === null) return -1;
  if (right === null) return 1;

  if (Array.isArray(left) && Array.isArray(right)) {
    const limit = Math.min(left.length, right.length);

    for (let index = 0; index < limit; index++) {
      const order = compare(left[index], right[index]);
      if (order !== 0) return order;
    }

    return left.length - right.length;
  }

  if (typeof left !== typeof right) {
    throw new Error(`Cannot compare ${typeof left} with ${typeof right}`);
  }

  if (left < right) return -1;
  if (left > right) return 1;

  return 0;
}
