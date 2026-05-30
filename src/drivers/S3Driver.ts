import { createWriteStream } from 'fs';
import { posix as posixPath } from 'path';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import type {
  S3Client,
  PutObjectCommandInput,
} from '@aws-sdk/client-s3';
import {
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  NotFound,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { Driver, StoreInput, StoreOptions } from './Driver';
import { tryCatch } from '../tryCatch';

export class S3Driver implements Driver {
  readonly client: S3Client;
  readonly bucket: string;
  readonly prefix: string | null;

  constructor({ client, bucket, prefix = null }: { client: S3Client, bucket: string, prefix?: string | null }) {
    this.client = client;
    this.bucket = bucket;
    this.prefix = prefix;
  }

  withPrefix(path: string): string {
    return this.prefix ? posixPath.join(this.prefix, path) : path;
  }

  async list({ prefix = null }: { prefix?: string | null } = {}): Promise<string[]> {
    const names: string[] = [];
    let continuationToken: string | undefined;

    do {
      const response = await this.client.send(new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: prefix ?? undefined,
        ContinuationToken: continuationToken,
      }));

      for (const object of response.Contents ?? []) {
        if (object.Key) names.push(object.Key);
      }

      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return names.sort();
  }

  async value(name: string): Promise<string> {
    const response = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: name }));
    if (!response.Body) throw new Error(`Empty body for ${name}`);

    return await response.Body.transformToString('utf8');
  }

  async download(name: string, path: string): Promise<void> {
    const response = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: name }));
    if (!response.Body) throw new Error(`Empty body for ${name}`);

    await pipeline(response.Body as Readable, createWriteStream(path));
  }

  async exists(name: string): Promise<boolean> {
    const [error] = await tryCatch(() => this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: name })));
    
    if (error instanceof NotFound) return false;
    if (error) throw error;

    return true;
  }

  async store(name: string, dataOrStream: StoreInput, options: StoreOptions = {}): Promise<void> {
    const body: PutObjectCommandInput['Body'] = dataOrStream as PutObjectCommandInput['Body'];

    await this.client.send(new PutObjectCommand({
      Bucket: this.bucket,
      Key: name,
      Body: body,
      ...(options as Partial<PutObjectCommandInput>),
    }));
  }
}
