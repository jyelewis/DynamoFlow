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
});
