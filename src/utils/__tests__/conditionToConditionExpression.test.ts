import { conditionToConditionExpression } from "../conditionToConditionExpression.js";

describe("conditionToConditionExpression", () => {
  // TODO: write rest of tests
  it("Basic eq", () => {
    expect(
      conditionToConditionExpression({
        firstName: "Joe",
      })
    ).toEqual({
      conditionExpression: "(#exp0 = :exp0)",
      expressionAttributeNames: {
        "#exp0": "firstName",
      },
      expressionAttributeValues: {
        ":exp0": "Joe",
      },
    });
  });

  it("$contains expression", () => {
    expect(
      conditionToConditionExpression({
        name: { $contains: "and sons" },
      })
    ).toEqual({
      conditionExpression: "(contains(#exp0, :exp0))",
      expressionAttributeNames: {
        "#exp0": "name",
      },
      expressionAttributeValues: {
        ":exp0": "and sons",
      },
    });
  });
});
