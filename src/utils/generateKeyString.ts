import { DynamoItem } from "../types/types.js";
import { valueToSortableString } from "./valueToSortableString.js";

export function generateKeyString(
  indexKeys: string | string[],
  entity: Partial<DynamoItem>,
): string {
  const indexKeysAsArray = Array.isArray(indexKeys) ? indexKeys : [indexKeys];
  const indexString = indexKeysAsArray
    .map((key) => {
      const value = entity[key];
      if (value === undefined) {
        throw new Error(`Missing required value for ${key} in index`);
      }

      return valueToSortableString(value);
    })
    .join("#");

  // all index segments end with #
  // this allows us to find all matching items with a strict eq comparison
  // by performing begins_with(#SK, :`value1#value2#`)
  if (indexString === "") {
    // dont add hash suffix if there are no keys
    return "";
  }
  return indexString + "#";
}
