import { DynamoItem } from "../types/types.js";
import { generateKeyString } from "./generateKeyString.js";

export function generateIndexStrings(
  collectionName: string,
  partitionKeys: string | string[],
  sortKeys: undefined | string | string[],
  entity: Partial<DynamoItem>,
): [string, string] {
  return [
    `${collectionName}#${generateKeyString(partitionKeys, entity)}`,
    `${collectionName}#${generateKeyString(sortKeys || [], entity)}`,
  ];
}
