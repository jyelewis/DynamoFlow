import { valueToSortableString } from "../valueToSortableString.js";

describe("valueToSortableString", () => {
  it("null", () => {
    expect(valueToSortableString(null)).toEqual("N:NULL");
  });

  it("strings", () => {
    expect(valueToSortableString("hello")).toEqual("s:hello");
    expect(valueToSortableString("h#ello")).toEqual("s:h\\#ello");
    // truncates long strings
    expect(valueToSortableString("long string ".repeat(50)).length).toEqual(
      102
    );
  });

  it("integers", () => {
    expect(valueToSortableString(-987654321)).toMatchInlineSnapshot(
      `"n:-0000gc0uy9.000000"`
    );
    expect(valueToSortableString(-1)).toMatchInlineSnapshot(
      `"n:-0000000001.000000"`
    );
    expect(valueToSortableString(0)).toMatchInlineSnapshot(
      `"n:0000000000.000000"`
    );
    expect(valueToSortableString(1)).toMatchInlineSnapshot(
      `"n:0000000001.000000"`
    );
    expect(valueToSortableString(123)).toMatchInlineSnapshot(
      `"n:000000003f.000000"`
    );
    expect(valueToSortableString(987654321)).toMatchInlineSnapshot(
      `"n:0000gc0uy9.000000"`
    );

    // need to be able to hold at least a unix ms timestamp
    expect(valueToSortableString(1687602125961)).toMatchInlineSnapshot(
      `"n:00lj9uqe89.000000"`
    );

    // truncates large number
    expect(
      // eslint-disable-next-line @typescript-eslint/no-loss-of-precision
      valueToSortableString(16879999999999999602125961)
    ).toMatchInlineSnapshot(`"n:24crhygif2.000000"`);
  });

  it("floats", () => {
    expect(valueToSortableString(-1.5)).toMatchInlineSnapshot(
      `"n:-0000000002.00apsw"`
    );
    expect(valueToSortableString(0.0)).toMatchInlineSnapshot(
      `"n:0000000000.000000"`
    );
    expect(valueToSortableString(1.0)).toMatchInlineSnapshot(
      `"n:0000000001.000000"`
    );
    expect(valueToSortableString(1.5)).toMatchInlineSnapshot(
      `"n:0000000001.00apsw"`
    );
    expect(valueToSortableString(123456.7891)).toMatchInlineSnapshot(
      `"n:0000002n9c.00gwvg"`
    );

    // truncates large fraction
    expect(valueToSortableString(0.12345678912345678)).toMatchInlineSnapshot(
      `"n:0000000000.002n9d"`
    );
  });

  it("booleans", () => {
    expect(valueToSortableString(true)).toEqual("b:1");
    expect(valueToSortableString(false)).toEqual("b:0");
  });

  it("Unknown types", () => {
    expect(() => valueToSortableString({} as any)).toThrow(
      "Value of type object is not a string, number, boolean or null"
    );
  });
});
