import { generateQueryExpression } from "../generateQueryExpression.js";
import { Query } from "../../types/types.js";

describe("generateQueryExpression", () => {
  it("Generates query expressions for simple eq queries", () => {
    const query = {
      where: {
        lastName: "Lewis",
        firstName: "Jye",
      },
    };
    const queryExpression = generateQueryExpression(
      "users",
      ["lastName"],
      ["firstName"],
      query
    );
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND begins_with(#SK, :value)",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "users#lewis#",
        ":value": "users#jye#",
      },
      indexName: undefined,
    });
  });

  it("Multiple eq values", () => {
    const query = {
      where: {
        a: "aa",
        b: "bb",
        c: "cc",
      },
    };
    const queryExpression = generateQueryExpression(
      "things",
      ["a", "b", "c"],
      undefined,
      query
    );
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND begins_with(#SK, :value)",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#bb#cc#",
        ":value": "things#",
      },
      indexName: undefined,
    });
  });

  it("$gt", () => {
    const query = {
      where: {
        a: "aa",
        b: { $gt: "b" },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND #SK > :value",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#b#",
      },
      indexName: undefined,
    });
  });

  it("$gte", () => {
    const query = {
      where: {
        a: "aa",
        b: { $gte: "b" },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND #SK >= :value",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#b#",
      },
      indexName: undefined,
    });
  });

  it("$lt", () => {
    const query = {
      where: {
        a: "aa",
        b: { $lt: "b" },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND #SK < :value",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#b#",
      },
      indexName: undefined,
    });
  });

  it("$lte", () => {
    const query = {
      where: {
        a: "aa",
        b: { $lte: "b" },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND #SK <= :value",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#b#",
      },
      indexName: undefined,
    });
  });

  it("$betweenIncl", () => {
    const query: Query<any> = {
      where: {
        a: "aa",
        b: { $betweenIncl: ["b", "z"] },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND #SK BETWEEN :gte AND :lte",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":gte": "things#b#",
        ":lte": "things#z#",
      },
      indexName: undefined,
    });
  });

  it("$beginsWith", () => {
    const query: Query<any> = {
      where: {
        a: "aa",
        b: { $beginsWith: "bb" },
      },
    };
    const queryExpression = generateQueryExpression("things", "a", "b", query);
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND begins_with(#SK, :value)",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#bb",
      },
      indexName: undefined,
    });
  });

  it("eq + $beginsWith", () => {
    const query: Query<any> = {
      where: {
        a: "aa",
        b: "b",
        c: { $beginsWith: "cc" },
      },
    };
    const queryExpression = generateQueryExpression(
      "things",
      "a",
      ["b", "c"],
      query
    );
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND begins_with(#SK, :value)",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#aa#",
        ":value": "things#b#cc",
      },
      indexName: undefined,
    });
  });

  it("A filtering on the full key value", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        b: "b",
        c: "c",
      },
    };
    const queryExpression = generateQueryExpression(
      "things",
      "a",
      ["b", "c"],
      query
    );
    expect(queryExpression).toEqual({
      keyConditionExpression: "#PK = :pk AND begins_with(#SK, :value)",
      expressionAttributeNames: {
        "#PK": "_PK",
        "#SK": "_SK",
      },
      expressionAttributeValues: {
        ":pk": "things#a#",
        ":value": "things#b#c#",
      },
      indexName: undefined,
    });
  });

  it("Throws if any partition key values are missing", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        c: "c",
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a", "b"], "c", query)
    ).toThrowError("Missing required value for b in index");
  });

  it("Throws if there is a hole in the sort key", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        c: "c",
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(
      "Cannot query on c after a range query or missing a previous parameter value"
    );
  });

  it("Throws if filtering with the range expression not at the end of the sort key", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        b: { $beginsWith: "b" },
        c: "a",
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(
      "Cannot query on c after a range query or missing a previous parameter value"
    );
  });

  it("Throws if multiple range conditions are provided", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        b: { $beginsWith: "b" },
        c: { $gt: "c" },
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(
      "Cannot query on c after a range query or missing a previous parameter value"
    );
  });

  it("Throws if partition key value is invalid", () => {
    const query: Query<any> = {
      where: {
        a: { someObj: 123 },
        b: "b",
        c: "c",
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(
      "Value of type object is not a string, number, boolean or null"
    );
  });

  it("Throws if partition key value is is range", () => {
    const query: Query<any> = {
      where: {
        a: { $gt: 123 },
        b: "b",
        c: "c",
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(
      "Value of type object is not a string, number, boolean or null"
    );
  });

  it("Throws if sort key value is invalid object", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        b: "b",
        c: { someObj: 123 },
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(`Invalid query where filter provided "c": [object Object]`);
  });

  it("Throws if sort key value is invalid type", () => {
    const query: Query<any> = {
      where: {
        a: "a",
        b: "b",
        c: function () {},
      },
    };
    expect(() =>
      generateQueryExpression("things", ["a"], ["b", "c"], query)
    ).toThrowError(`Invalid query where filter provided "c": function () { }`);
  });
});
