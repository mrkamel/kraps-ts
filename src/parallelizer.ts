import { tryCatch } from './tryCatch';

export async function parallelEach<T>(
  items: Iterable<T>,
  concurrency: number,
  handler: (item: T) => Promise<void>,
): Promise<void> {
  const iterator = items[Symbol.iterator]();
  let firstError: Error | null = null;

  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (firstError === null) {
      const next = iterator.next();

      if (next.done) return;

      const [error] = await tryCatch(() => handler(next.value));

      if (error) {
        if (firstError === null) firstError = error;

        return;
      }
    }
  });

  await Promise.all(workers);

  if (firstError) throw firstError;
}
