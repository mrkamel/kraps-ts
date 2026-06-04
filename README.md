# kraps

**Easily process big data in TypeScript/Node**

Kraps allows you to process and perform calculations on very large datasets in
parallel using a map/reduce framework similar to [Spark](https://spark.apache.org/),
but runs on a background job framework you already have. You just need some
space on your filesystem, S3 as a storage layer (with a temporary lifecycle
policy enabled), a background job framework, and Redis to track progress.

## Install

```bash
npm install kraps ioredis
# optional: only if you use the S3 driver
npm install @aws-sdk/client-s3
```

## Configure

```ts
import { Redis } from 'ioredis';
import { S3Client } from '@aws-sdk/client-s3';
import { configure, S3Driver } from 'kraps';

configure({
  driver: new S3Driver({
    client: new S3Client({ region: 'eu-central-1' }),
    bucket: 'some-bucket',
    prefix: 'temp/kraps/',
  }),
  redis: new Redis(),
  namespace: 'my-application',  // optional, used as a redis key prefix
  jobTtl: 7 * 24 * 60 * 60,     // optional, default 4 days (seconds)
  showProgress: true,            // optional, default true; prints a TTY progress bar per step
  enqueuer: async (json) => {
    // hand off the job to your background queue
    await myQueue.add('KrapsWorker', { json });
  },
  jobs: [SearchLogCounter],  // see "Define a job" below
});
```

## Define a job

Pipelines are built by chaining steps on a `Job`. Each step's block returns an
`Iterable` (or `AsyncIterable`) of emitted items — sync arrays for small,
eager outputs and generators (`function*` / `async function*`) for lazy
production.

```ts
import { defineJob, Job } from 'kraps';

const SearchLogCounter = defineJob({
  name: 'SearchLogCounter',
  job: (startDate: string, endDate: string) => {
    return new Job()
      .parallelize(function* () {
        for (let date = new Date(startDate); date <= new Date(endDate); date.setDate(date.getDate() + 1)) {
          yield date.toISOString().slice(0, 10);
        }
      }, { partitions: 128 })
      .map(async function* (date) {
        const lines = await fetchLogFile(date);

        for (const line of lines) {
          const parsed = JSON.parse(line);
          yield [parsed.q, 1] as [string, number];
        }
      })
      .reduce((_key, leftCount, rightCount) => leftCount + rightCount)
      .eachPartition(async (partition, pairs) => {
        const lines: string[] = [];

        for await (const [query, count] of pairs) {
          lines.push(JSON.stringify({ q: query, count }));
        }

        await uploadToS3(`results/${partition}.jsonl`, lines.join('\n'));
      });
  },
});
```

Type inference flows through the chain — `parallelize` produces a
`Job<string, null>`, `map` produces `Job<string, number>`, and `eachPartition`
sees `pairs: AsyncIterable<[string, number]>`. No `as` casts needed on the
keys/values.

**Pipeline registration:** the worker process rebuilds the job graph from the
payload's `name`, so every job you run must appear in the `jobs` array passed
to `configure()`. `defineJob` enforces both `name` and `job` at the
definition site — the string literal `name` is the identifier sent over the
wire, kept stable across bundler minification and HMR.

## Worker

The `enqueuer` you configure receives a JSON payload and is responsible for
handing it off to a background queue. In the worker process, instantiate
`Worker` to handle that payload:

```ts
import { Worker } from 'kraps';

async function handleKrapsJob(json: string) {
  const worker = new Worker(json, {
    memoryLimit: 16 * 1024 * 1024,  // bytes
    chunkLimit: 64,
    concurrency: 8,
  });

  await worker.run({ retries: 3 });
}
```

* `memoryLimit` — how large a single in-memory chunk may grow before it spills
  to a temp file (gzipped, line-delimited JSON).
* `chunkLimit` — caps the number of files open during k-way merges.
* `concurrency` — parallelism for storage I/O (uploads/downloads).

## Run

```ts
import { createJob } from 'kraps';

await createJob(SearchLogCounter).run('2018-01-01', '2022-01-01');
```

Positional `run` arguments are inferred from the `job` signature, so the
call is type-checked end-to-end.

## Job API

Every step method takes the **block first, options second**. Options are
optional where every field has a default.

| Method | Block signature | Block returns |
| --- | --- | --- |
| `parallelize(block, { partitions, partitioner?, enqueuer?, before? })` | `() => …` | `Iterable<NewKey> \| AsyncIterable<NewKey>` |
| `map(block, { partitions?, partitioner?, jobs?, enqueuer?, before? }?)` | `(key, value) => …` | `Iterable<[NewKey, NewValue]> \| AsyncIterable<…>` |
| `mapPartitions(block, { … }?)` | `(partition, pairs) => …` (pairs is `AsyncIterable<[Key, Value]>`, sorted) | same as `map` |
| `reduce(block, { jobs?, enqueuer?, before? }?)` | `(key, leftValue, rightValue) => Value \| Promise<Value>` | a single merged value |
| `combine(otherJob, block, { jobs?, enqueuer?, before? }?)` | `(key, leftValue, rightValue \| null) => …` (right is `null` when no match) | `Iterable<[Key, ResultValue]> \| AsyncIterable<…>` |
| `append(otherJob, { jobs?, enqueuer?, before? }?)` | — (no block) | — |
| `eachPartition(block, { jobs?, enqueuer?, before? }?)` | `(partition, pairs) => …` | `void \| Promise<void>` (side effects only) |
| `repartition({ partitions, partitioner?, jobs?, enqueuer?, before? })` | — | — |
| `dump({ prefix, enqueuer? })` | — | per-partition file written under `prefix/<n>/chunk.json` |
| `load({ prefix, partitions, partitioner, concurrency, enqueuer? })` | — | seeds a fresh job from previously dumped data |

`partitioner` defaults to `hashPartitioner`. `jobs` caps the number of
wake-ups the runner pushes for that step (one wake-up triggers one
`Worker.run`; if your workers are long-lived and drain the queue per run,
set `jobs` close to your worker concurrency to avoid no-op wake-ups).

`combine` combines the results of two jobs by joining every key available in
the current job with the corresponding key from `otherJob`. When `otherJob`
does not have a corresponding key, `null` is passed to the block. **Keys which
are only available in `otherJob` are completely omitted** (left-outer join,
not full-outer). The keys, partitioners and number of partitions must match
between the two jobs, and `otherJob` must be reduced (every key unique).
`otherJob` does not need to be listed in the array returned from `run()` —
kraps detects the dependency.

`append` requires the partitioners and number of partitions to match between
the two jobs.

## Type safety

`Job<Key, Value>` is generic over the current step's key/value type, and the
methods narrow these as you chain. `KrapsKey` (sortable JSON-safe values:
`string | number | boolean | null | KrapsKey[]`) is the constraint on keys;
`JsonValue` is the constraint on values.

If TypeScript can't infer the new types from the block (commonly when the
block yields no concrete data, or yields literals that need widening), pass
the type arguments explicitly:

```ts
.parallelize<string>(/* block */, { partitions: 8 })
.map<string, number>(/* block */)
```

## Datatypes

All keys and values round-trip through JSON. Keys must be sortable — strings,
numbers, booleans, `null`, and arrays of those work; objects do not have a
stable comparison order and are intentionally excluded from `KrapsKey`. The
default `hashPartitioner` takes the first 5 hex digits of
`SHA1(JSON.stringify(key))` and returns the result modulo `numPartitions`.

## Storage

Kraps stores temporary results in the configured driver. The S3 driver
expects you to set up a lifecycle policy on the bucket (or a prefix) to delete
stale objects, since kraps itself does not clean up — leaving rubbish behind
is safer than risking premature deletion on error.

```ts
new S3Driver({
  client: new S3Client({ /* ... */ }),
  bucket: 'some-bucket',
  prefix: 'temp/kraps/',
});
```

For tests, use `FakeDriver`:

```ts
import { FakeDriver } from 'kraps';

configure({ driver: new FakeDriver({ bucket: 'test' }), redis: new Redis({ db: 15 }) });
```

## End-to-end example

See `e2e/distributed/` for a fully distributed wordcount pipeline using
real Redis + rustfs (S3-compatible) + multiple worker processes. Bring up the
services with `docker compose up`, start one or more workers
(`npx tsx e2e/distributed/worker.ts` in separate terminals), and run the
producer (`npx tsx e2e/distributed/server.ts`).

## License

MIT
