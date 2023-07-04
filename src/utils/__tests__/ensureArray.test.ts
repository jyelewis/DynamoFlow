import { ensureArray } from "../ensureArray.js";

describe("ensureArray", () => {
  it("undefined", () => {
    expect(ensureArray(undefined)).toEqual([]);
    expect(ensureArray()).toEqual([]);
  });

  it("empty array", () => {
    expect(ensureArray([])).toEqual([]);
  });

  it("string literals", () => {
    expect(ensureArray("one")).toEqual(["one"]);
    expect(ensureArray(["one"])).toEqual(["one"]);
    expect(ensureArray(["one", "two"])).toEqual(["one", "two"]);
  });

  it("number literal", () => {
    expect(ensureArray(123)).toEqual([123]);
    expect(ensureArray([123])).toEqual([123]);
    expect(ensureArray([123, 456])).toEqual([123, 456]);
  });

  it("object literal", () => {
    expect(ensureArray({ a: 1 })).toEqual([{ a: 1 }]);
    expect(ensureArray([{ a: 1 }])).toEqual([{ a: 1 }]);
    expect(ensureArray([{ a: 1 }, { b: 2 }])).toEqual([{ a: 1 }, { b: 2 }]);
  });
});
