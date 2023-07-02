import { DynamoValue } from "../types/types.js";

// Check this isn't a query or expression object. If not, assume it is a literal DynamoValue
// query expression objects look like: '{ $eq: 4 }'
export function isDynamoValue(x: any): x is DynamoValue {
  return !(
    typeof x === "object" &&
    x !== null &&
    Object.keys(x).length > 0 &&
    Object.keys(x)[0]?.startsWith("$")
  );
}
