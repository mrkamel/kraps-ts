import { TempPath } from './TempPath';

export class TempPaths {
  private readonly paths: TempPath[] = [];

  async add(): Promise<TempPath> {
    const tempPath = await TempPath.create();
    this.paths.push(tempPath);
    return tempPath;
  }

  async delete(): Promise<void> {
    await Promise.all(this.paths.map((tempPath) => tempPath.delete()));
  }

  toArray(): TempPath[] {
    return [...this.paths];
  }

  [Symbol.iterator](): Iterator<TempPath> {
    return this.paths[Symbol.iterator]();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.delete();
  }
}
