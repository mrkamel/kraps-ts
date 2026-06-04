import { describe, it, expect } from 'vitest';
import { defineJob } from '../src/KrapsJob';

describe('defineJob', () => {
  it('returns the declaration when valid', () => {
    const declaration = defineJob({
      name: 'Valid',
      job() { return null as any; },
    });

    expect(declaration.name).toBe('Valid');
    expect(typeof declaration.job).toBe('function');
  });

  it('throws when name is missing', () => {
    expect(() => defineJob({ job() { return null as any; } } as any))
      .toThrow(/requires a name/);
  });

  it('throws when name is an empty string', () => {
    expect(() => defineJob({ name: '', job() { return null as any; } }))
      .toThrow(/non-empty string/);
  });

  it('throws when job is not a function', () => {
    expect(() => defineJob({ name: 'Bad' } as any))
      .toThrow(/requires a job function/);
  });
});
