import { describe, it, expect } from 'vitest';
import { access } from 'fs/promises';
import { TempPaths } from '../src/TempPaths';

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

describe('TempPaths', () => {
  describe('add', () => {
    it('adds a temp path', async () => {
      await using tempPaths = new TempPaths();

      await tempPaths.add();
      await tempPaths.add();

      expect(tempPaths.toArray()).toHaveLength(2);
    });

    it('returns the added temp path', async () => {
      await using tempPaths = new TempPaths();
      const tempPath = await tempPaths.add();

      expect(tempPaths.toArray().at(-1)).toBe(tempPath);
    });
  });

  describe('delete', () => {
    it('deletes all temp path files', async () => {
      const tempPaths = new TempPaths();

      const first = await tempPaths.add();
      const second = await tempPaths.add();

      expect(await exists(first.path)).toBe(true);
      expect(await exists(second.path)).toBe(true);

      await tempPaths.delete();

      expect(await exists(first.path)).toBe(false);
      expect(await exists(second.path)).toBe(false);
    });
  });

  describe('iteration', () => {
    it('yields each temp path in insertion order', async () => {
      await using tempPaths = new TempPaths();

      const first = await tempPaths.add();
      const second = await tempPaths.add();

      expect([...tempPaths]).toEqual([first, second]);
    });
  });
});
