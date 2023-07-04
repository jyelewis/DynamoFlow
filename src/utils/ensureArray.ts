export function ensureArray<T>(value?: T | T[]): T[] {
  if (value === undefined) {
    return [];
  }

  return Array.isArray(value) ? value : [value];
}
