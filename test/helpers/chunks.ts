import { gzipSync, gunzipSync } from 'zlib';

export function gzipPairLines(pairs: [unknown, unknown][]): Buffer {
  const body = pairs.map((pair) => JSON.stringify(pair)).join('\n') + '\n';

  return gzipSync(body);
}

export function gunzipPairLines(buffer: Buffer): [unknown, unknown][] {
  const text = gunzipSync(buffer).toString('utf8').trimEnd();

  if (text.length === 0) return [];

  return text.split('\n').map((line) => JSON.parse(line) as [unknown, unknown]);
}
