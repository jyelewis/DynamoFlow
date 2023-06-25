// TODO: support set & dict operations
import { ScanCommandInput } from "@aws-sdk/lib-dynamodb";
import { DFCollection } from "../DFCollection.js";

export type DynamoValue = string | number | boolean | null;
export type DynamoItem = Record<string, DynamoValue>;
export type UpdateValue = DynamoValue | { $inc: number } | { $remove: true };

export const RETRY_TRANSACTION = Symbol("RETRY_TRANSACTION");
export const STOP_SCAN = Symbol("STOP_SCAN");

export type SafeEntity<Entity> = {
  [K in keyof Entity]: DynamoValue;
};

export type EntityWithMetadata<Entity> = Entity &
  Record<`_${string}`, DynamoValue>;

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
  maxBatchSize?: number;
} & (
  | Record<string, never>
  | {
      filterExpression?: ScanCommandInput["FilterExpression"];
      filterExpressionAttributeNames?: ScanCommandInput["ExpressionAttributeNames"];
      filterExpressionAttributeValues?: ScanCommandInput["ExpressionAttributeValues"];
    }
);
