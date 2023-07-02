// TODO: support set & dict operations
import { DFCollection } from "../DFCollection.js";
import { DFConditionValue } from "./operations.js";

export type SimpleDynamoValue =
  | string
  | number
  | boolean
  | null
  | Set<string>
  | Set<number>;

export type DynamoValue =
  | string
  | number
  | boolean
  | null // literal or set
  | SimpleDynamoValue[] // list
  // TODO: start with support for these
  | Record<string, SimpleDynamoValue> // dict
  | Record<string, SimpleDynamoValue>[]; // list of dicts

export type DynamoItem = Record<string, DynamoValue>;
export type UpdateValue =
  | DynamoValue
  | { $inc: number }
  | { $remove: true }
  | { $setIfNotExists: DynamoValue };
// TODO: implement me (don't forget GSIs!)
// | { $addToSet: string[] | number[] }
// | { $removeFromSet: string[] | number[] }
// | { $addToList: SimpleDynamoValue[] }
// | { $removeFromList: number[] }; // indexes to remove

export const RETRY_TRANSACTION = Symbol("RETRY_TRANSACTION");
export const STOP_SCAN = Symbol("STOP_SCAN");

export type SafeEntity<Entity> = {
  [K in keyof Entity]: K extends string ? DynamoValue : never;
};

// export type EntityWithMetadata = Entity & Record<string, DynamoValue>;
export type EntityWithMetadata = Record<string, DynamoValue>;

export interface Query<Entity extends SafeEntity<Entity>> {
  where: {
    [key in keyof Entity]?:
      | Entity[key]
      | { $betweenIncl: [Entity[key], Entity[key]] }
      | { $lt: Entity[key] }
      | { $lte: Entity[key] }
      | { $gt: Entity[key] }
      | { $gte: Entity[key] }
      | { $beginsWith: Entity[key] };
  };
  filter?: Partial<Record<keyof Entity, DFConditionValue>>;
  limit?: number;
  consistentRead?: boolean;
  index?: string;
}

export interface FullTableScanItem {
  collection?: DFCollection<any>;
  entity: SafeEntity<any>;
}
export type FullTableScan = {
  processBatch: (
    items: FullTableScanItem[]
  ) => Promise<void | typeof STOP_SCAN>;
  filter?: Record<string, DFConditionValue>;
  maxBatchSize?: number;
  segment?: number;
  totalSegments?: number;
};
