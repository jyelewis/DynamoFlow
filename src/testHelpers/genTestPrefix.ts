export function genTestPrefix(): string {
  return Math.random().toString(32).split(".")[1];
}
