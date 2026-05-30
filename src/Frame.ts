export type Frame = {
  token: string,
  partitions: number,
};

export type SerializedFrame = Frame | Record<string, never>;
