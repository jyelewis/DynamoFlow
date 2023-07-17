import { DFCollection } from "../DFCollection.js";
import { DFConditionValue } from "./operations.js";
import { PartialQueryExpression } from "./internalTypes.js";

// export type SimpleDynamoValue =
//   | string
//   | number
//   | boolean
//   | null
//   | Set<string>
//   | Set<number>;

// export type DynamoValue =
//   | string
//   | number
//   | boolean
//   | null // literal or set
//   | SimpleDynamoValue[] // list
//   | Record<string, SimpleDynamoValue> // dict
//   | Record<string, SimpleDynamoValue>[] // list of dicts
//   | Set<string>
//   | Set<number>;

export type DynamoValue =
  | string
  | number
  | boolean
  | null
  | DynamoValue[]
  | { [key: string | number]: DynamoValue }
  | Set<string>
  | Set<number>;

export type DynamoItem = Record<string, DynamoValue>;
export type UpdateValue =
  | undefined
  | DynamoValue
  | { $inc: number }
  | { $remove: true }
  | { $setIfNotExists: DynamoValue }
  | { $addItemsToSet: Set<string> | Set<number> }
  | { $removeItemsFromSet: Set<string> | Set<number> }
  | {
      $appendItemsToList: Array<DynamoValue>;
    };

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
      | { $beginsWith: Entity[key] }
      | { $custom: any };
  };
  filter?: Partial<Record<keyof Entity, DFConditionValue>>;
  limit?: number;
  consistentRead?: boolean;
  index?: string;
  returnRaw?: boolean;
  exclusiveStartKey?: any;
  sort?: "ASC" | "DESC";
  rawExpression?: PartialQueryExpression;
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
  dynamoIndex?: string;
  returnRaw?: boolean; // useful for forcing migrations
  segment?: number;
  totalSegments?: number;
};
