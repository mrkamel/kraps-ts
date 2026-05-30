import { tmpdir } from 'os';
import { join } from 'path';
import { randomBytes } from 'crypto';
import { rm, writeFile } from 'fs/promises';

export class TempPath {
  readonly path: string;

  private constructor(path: string) {
    this.path = path;
  }

  static async create(): Promise<TempPath> {
    const name = `kraps-${process.pid}-${Date.now()}-${randomBytes(8).toString('hex')}`;
    const path = join(tmpdir(), name);

    await writeFile(path, '');

    return new TempPath(path);
  }

  async delete(): Promise<void> {
    await rm(this.path, { force: true });
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.delete();
  }
}
