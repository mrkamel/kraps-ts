import { getConfig } from './config';
import { parallelEach } from './parallelizer';
import { TempPath } from './TempPath';
import { TempPaths } from './TempPaths';

export async function downloadAll({ prefix, concurrency }: { prefix: string, concurrency: number }): Promise<TempPaths> {
  const driver = getConfig().driver;
  const files = (await driver.list({ prefix })).sort();

  const tempPaths = new TempPaths();
  const indexByFile = new Map<string, TempPath>();

  for (const file of files) {
    indexByFile.set(file, await tempPaths.add());
  }

  await parallelEach(files, concurrency, async (file) => {
    const tempPath = indexByFile.get(file);
    if (!tempPath) return;

    await driver.download(file, tempPath.path);
  });

  return tempPaths;
}
