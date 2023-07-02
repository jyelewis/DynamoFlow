import { isDynamoValue } from "../isDynamoValue.js";

describe("isDynamoValue", () => {
  it("Handles literal values", () => {
    expect(isDynamoValue(1)).toBe(true);
    expect(isDynamoValue("blah")).toBe(true);
    expect(isDynamoValue(null)).toBe(true);
    expect(isDynamoValue(false)).toBe(true);
    expect(isDynamoValue(true)).toBe(true);
    expect(isDynamoValue({ a: 1 })).toBe(true);
    expect(isDynamoValue([1, 2, 3])).toBe(true);
  });

  it("Handles query/expression values", () => {
    expect(isDynamoValue({ $eq: 4 })).toBe(false);
    expect(isDynamoValue({ $betweenIncl: [4, 7] })).toBe(false);
    expect(isDynamoValue({ $param1: "a", $param2: "b" })).toBe(false);
  });
});
