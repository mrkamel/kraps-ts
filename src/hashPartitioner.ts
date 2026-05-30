import { createHash } from 'crypto';
import { KrapsKey } from './mapReduce/compare';

export type Partitioner<Key extends KrapsKey = KrapsKey> = (key: Key, numPartitions: number) => number;

export const hashPartitioner: Partitioner = (key, numPartitions) => {
  if (numPartitions <= 0) throw new Error(`hashPartitioner: numPartitions must be > 0, got ${numPartitions}`);

  const hex = createHash('sha1').update(JSON.stringify(key)).digest('hex').slice(0, 5);
  return parseInt(hex, 16) % numPartitions;
};
