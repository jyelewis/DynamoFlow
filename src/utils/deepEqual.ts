// https://stackoverflow.com/questions/25456013/javascript-deepequal-comparison
export function deepEqual(x: any, y: any): boolean {
  if (x === y) {
    return true;
  }

  if (typeof x == "object" && x != null && typeof y == "object" && y != null) {
    if (Object.keys(x).length != Object.keys(y).length) return false;

    for (const prop in x) {
      if (Object.prototype.hasOwnProperty.call(y, prop)) {
        if (!deepEqual(x[prop], y[prop])) return false;
      } else return false;
    }

    return true;
  }

  return false;
}
