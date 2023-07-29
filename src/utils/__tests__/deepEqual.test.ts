import { deepEqual } from "../deepEqual.js";

describe("deepEqual", () => {
  it("literal values", () => {
    expect(deepEqual(1, 1)).toEqual(true);
    expect(deepEqual(1, 2)).toEqual(false);
    expect(deepEqual("a", "a")).toEqual(true);
    expect(deepEqual("a", "b")).toEqual(false);
    expect(deepEqual(true, true)).toEqual(true);
    expect(deepEqual(true, false)).toEqual(false);
    expect(deepEqual(null, null)).toEqual(true);
    expect(deepEqual(null, undefined)).toEqual(false);
    expect(deepEqual(undefined, undefined)).toEqual(true);
  });

  it("Object values", () => {
    expect(deepEqual({ a: 1 }, { a: 1 })).toEqual(true);
    expect(deepEqual({ a: 1 }, { a: 2 })).toEqual(false);
    expect(deepEqual({ a: 1 }, { b: 1 })).toEqual(false);
    expect(deepEqual({ a: 1 }, { a: 1, b: 2 })).toEqual(false);
    expect(deepEqual({ a: 1, b: 2 }, { a: 1, b: 2 })).toEqual(true);
    expect(deepEqual({ a: 1, b: 2 }, { a: 1, b: 3 })).toEqual(false);
    expect(deepEqual({ a: 1, b: 2 }, { a: 1, c: 2 })).toEqual(false);
    expect(deepEqual({ a: 1, b: 2 }, { a: 1, b: 2, c: 3 })).toEqual(false);
    expect(deepEqual({ a: 1, b: 2, c: 3 }, { a: 1, b: 2, c: 3 })).toEqual(true);

    expect(deepEqual({ a: { b: "1" } }, { a: { b: "1" } })).toEqual(true);
    expect(deepEqual({ a: { b: "1" } }, { a: { b: "2" } })).toEqual(false);
  });

  it("Array values", () => {
    expect(deepEqual([1], [1])).toEqual(true);
    expect(deepEqual([1], [2])).toEqual(false);
    expect(deepEqual([1], [1, 2])).toEqual(false);
    expect(deepEqual([1, 2], [1, 2])).toEqual(true);
    expect(deepEqual([1, 2], [1, 3])).toEqual(false);

    expect(deepEqual([{ a: 1 }, { b: 2 }], [{ a: 1 }, { b: 2 }])).toEqual(true);
    expect(deepEqual([{ a: 1 }, { b: 2 }], [{ a: 1 }, { b: 3 }])).toEqual(
      false
    );
  });
});
