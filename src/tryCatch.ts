export function tryCatch<T>(fn: () => Promise<T>): Promise<[null, T]> | Promise<[Error, null]>;
export function tryCatch<T>(fn: () => T): [null, T] | [Error, null];

export function tryCatch(fn: any) {
  let res;

  try {
    res = fn();
  } catch (err) {
    return [errorify(err), null];
  }

  if (res instanceof Promise) {
    return new Promise((resolve) => {
      res.then((value) => resolve([null, value]))
        .catch((err) => resolve([errorify(err), null]));
    });
  }

  return [null, res];
}

function errorify(err: any): Error {
  if (err instanceof Error) return err;

  return new Error(`Invalid error: ${err}`);
}
