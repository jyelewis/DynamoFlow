import {
  ConditionExpressionProperties,
  DFCondition,
} from "../types/operations.js";
import { DynamoValue } from "../types/types.js";

// TODO: test me (+ query, insert, update, scan)
// converts our DFCondition interface to Dynamo conditions
// not super flexible, but can always fallback to "raw"
// TODO: allow raw update expressions too

export function conditionToConditionExpression(
  condition: undefined | DFCondition
): ConditionExpressionProperties {
  if (condition === undefined || Object.keys(condition).length === 0) {
    return {
      conditionExpression: undefined,
      expressionAttributeNames: undefined,
      expressionAttributeValues: undefined,
    };
  }

  const expressionAttributeNames: Record<string, string> = {};
  const expressionAttributeValues: Record<string, DynamoValue> = {};
  const expressionParts: string[] = [];
  Object.entries(condition).forEach(([key, conditionValue], index) => {
    expressionAttributeNames[`#exp${index}`] = key;
    // process special ($blah) values first

    if (typeof conditionValue === "object" && conditionValue !== null) {
      if ("$eq" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$eq;
        expressionParts.push(`#exp${index} = :exp${index}`);
        return;
      }
      if ("$ne" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$ne;
        expressionParts.push(`#exp${index} <> :exp${index}`);
        return;
      }
      if ("$exists" in conditionValue) {
        if (conditionValue["$exists"]) {
          expressionParts.push(`attribute_exists(#exp${index})`);
        } else {
          expressionParts.push(`attribute_not_exists(#exp${index})`);
        }
        return;
      }
      if ("$gt" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$gt;
        expressionParts.push(`#exp${index} > :exp${index}`);
        return;
      }
      if ("$gte" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$gte;
        expressionParts.push(`#exp${index} >= :exp${index}`);
        return;
      }
      if ("$lt" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$lt;
        expressionParts.push(`#exp${index} < :exp${index}`);
        return;
      }
      if ("$lte" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$lte;
        expressionParts.push(`#exp${index} <= :exp${index}`);
        return;
      }
      if ("$beginsWith" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$beginsWith;
        expressionParts.push(`begins_with(#exp${index}, :exp${index})`);
        return;
      }
      if ("$betweenIncl" in conditionValue) {
        expressionAttributeValues[`:exp${index}-lte`] =
          conditionValue.$betweenIncl[0];
        expressionAttributeValues[`:exp${index}-gte`] =
          conditionValue.$betweenIncl[1];
        expressionParts.push(
          `#exp${index} BETWEEN :exp${index}-lte AND :exp${index}-gte`
        );
        return;
      }
      if ("$in" in conditionValue) {
        const inValues = conditionValue.$in;
        const expressionItems: string[] = [];
        inValues.forEach((inValue, index) => {
          expressionAttributeValues[`:exp${index}-${index}`] = inValue;
          expressionItems.push(`:exp${index}-${index}`);
        });

        expressionParts.push(`#exp${index} IN ${expressionItems.join(",")}`);
        return;
      }
      if ("$contains" in conditionValue) {
        expressionAttributeValues[`:exp${index}`] = conditionValue.$contains;
        expressionParts.push(`contains(#exp${index}, :exp${index})`);
        return;
      }
      if ("$raw" in conditionValue) {
        // don't want to send extra values
        // key is meaningless here
        delete expressionAttributeNames[`#exp${index}`];

        Object.assign(
          expressionAttributeNames,
          conditionValue.$raw.expressionAttributeNames
        );
        Object.assign(
          expressionAttributeValues,
          conditionValue.$raw.expressionAttributeValues
        );
        expressionParts.push(conditionValue.$raw.conditionExpression);
        return;
      }

      throw new Error(`Unknown filter/condition '${key}: ${conditionValue}'`);
    }

    // raw value, default to $eq
    expressionAttributeValues[`:exp${index}`] = conditionValue;
    expressionParts.push(`#exp${index} = :exp${index}`);
  });

  return {
    conditionExpression: `(${expressionParts.join(") AND (")})`,
    expressionAttributeNames: expressionAttributeNames,
    expressionAttributeValues:
      Object.keys(expressionAttributeValues).length === 0
        ? undefined
        : expressionAttributeValues,
  };
}