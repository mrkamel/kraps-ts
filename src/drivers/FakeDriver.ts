import { writeFile } from 'fs/promises';
import { posix as posixPath } from 'path';
import { Driver, StoreInput, StoreOptions } from './Driver';

async function consume(input: StoreInput): Promise<Buffer> {
  if (typeof input === 'string') return Buffer.from(input);
  if (Buffer.isBuffer(input)) return input;

  const chunks: Buffer[] = [];

  for await (const chunk of input) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

export class FakeDriver implements Driver {
  readonly bucket: string;
  readonly prefix: string | null;
  readonly objects: Map<string, Buffer> = new Map();

  constructor({ bucket, prefix = null }: { bucket: string, prefix?: string | null }) {
    this.bucket = bucket;
    this.prefix = prefix;
  }

  withPrefix(path: string): string {
    return this.prefix ? posixPath.join(this.prefix, path) : path;
  }

  async list({ prefix = null }: { prefix?: string | null } = {}): Promise<string[]> {
    const names = Array.from(this.objects.keys());
    const filtered = prefix ? names.filter((name) => name.startsWith(prefix)) : names;

    return filtered.sort();
  }

  async value(name: string): Promise<string> {
    const buffer = this.objects.get(name);
    if (buffer === undefined) throw new Error(`No such object: ${name}`);

    return buffer.toString('utf8');
  }

  async download(name: string, path: string): Promise<void> {
    const buffer = this.objects.get(name);
    if (buffer === undefined) throw new Error(`No such object: ${name}`);

    await writeFile(path, buffer);
  }

  async exists(name: string): Promise<boolean> {
    return this.objects.has(name);
  }

  async store(name: string, dataOrStream: StoreInput, _options: StoreOptions = {}): Promise<void> {
    this.objects.set(name, await consume(dataOrStream));
  }

  flush(): void {
    this.objects.clear();
  }
}
