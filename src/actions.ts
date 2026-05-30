export const Actions = {
  PARALLELIZE: 'parallelize',
  MAP: 'map',
  MAP_PARTITIONS: 'map_partitions',
  REDUCE: 'reduce',
  COMBINE: 'combine',
  EACH_PARTITION: 'each_partition',
  APPEND: 'append',
} as const;

export type Action = typeof Actions[keyof typeof Actions];
export const ALL_ACTIONS: readonly Action[] = Object.values(Actions);
