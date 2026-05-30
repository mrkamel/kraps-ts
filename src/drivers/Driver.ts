import { Readable } from 'stream';

export type StoreOptions = Record<string, unknown>;
export type StoreInput = string | Buffer | Readable;

export interface Driver {
  readonly bucket: string;
  readonly prefix: string | null;

  withPrefix(path: string): string;
  list(options?: { prefix?: string | null }): Promise<string[]>;
  value(name: string): Promise<string>;
  download(name: string, path: string): Promise<void>;
  exists(name: string): Promise<boolean>;
  store(name: string, dataOrStream: StoreInput, options?: StoreOptions): Promise<void>;
}
