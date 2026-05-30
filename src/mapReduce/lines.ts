import { createReadStream, createWriteStream } from 'fs';
import { createGunzip, createGzip, Gzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable, Writable } from 'stream';
import { once } from 'events';
import { createInterface } from 'readline';

export async function* readLines(filePath: string): AsyncGenerator<string> {
  const fileStream = createReadStream(filePath);
  const reader = createInterface({ input: fileStream, crlfDelay: Infinity });

  try {
    for await (const line of reader) {
      yield line;
    }
  } finally {
    reader.close();
    fileStream.destroy();
  }
}

export async function writeLines(filePath: string, lines: AsyncIterable<string> | Iterable<string>): Promise<void> {
  const source = Readable.from((async function* () {
    for await (const line of lines as AsyncIterable<string>) {
      yield `${line}\n`;
    }
  })());

  await pipeline(source, createWriteStream(filePath));
}

export async function* readGzippedLines(filePath: string): AsyncGenerator<string> {
  const fileStream = createReadStream(filePath);
  const gunzip = fileStream.pipe(createGunzip());
  const reader = createInterface({ input: gunzip, crlfDelay: Infinity });

  try {
    for await (const line of reader) {
      yield line;
    }
  } finally {
    reader.close();
    gunzip.destroy();
    fileStream.destroy();
  }
}

export async function writeGzippedLines(filePath: string, lines: AsyncIterable<string> | Iterable<string>): Promise<void> {
  const source = Readable.from((async function* () {
    for await (const line of lines as AsyncIterable<string>) {
      yield `${line}\n`;
    }
  })());

  await pipeline(source, createGzip(), createWriteStream(filePath));
}

export class GzippedLineWriter {
  private readonly gzip: Gzip;
  private readonly fileStream: Writable;
  private readonly finished: Promise<void>;

  constructor(filePath: string) {
    this.gzip = createGzip();
    this.fileStream = createWriteStream(filePath);
    this.finished = pipeline(this.gzip, this.fileStream);
  }

  async write(line: string): Promise<void> {
    if (!this.gzip.write(`${line}\n`)) {
      await once(this.gzip, 'drain');
    }
  }

  async close(): Promise<void> {
    this.gzip.end();
    await this.finished;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
