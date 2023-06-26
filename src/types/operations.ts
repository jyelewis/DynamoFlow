import { DynamoItem, DynamoValue, UpdateValue } from "./types.js";
import { CancellationReason } from "@aws-sdk/client-dynamodb";

export interface DFConditionExpression {
  // TODO: this is going to need to be regularlly extended, maybe string concats aren't the best idea
  conditionExpression: string;
  conditionExpressionAttributeNames: Record<string, string>;
  conditionExpressionAttributeValues?: Record<string, DynamoValue>;
}
type DFOptionalConditionExpression =
  | DFConditionExpression
  | {
      conditionExpression?: never;
      conditionExpressionAttributeNames?: never;
      conditionExpressionAttributeValues?: never;
    };
export type DFWriteTransactionErrorHandler<Operation extends DFWriteOperation> =
  (error: CancellationReason, op: Operation) => symbol | Promise<symbol>;

export type DFUpdateOperation = {
  type: "Update";
  key: Record<string, DynamoValue>;
  updateValues: Record<string, UpdateValue>;
  successHandlers?: Array<(item: DynamoItem) => void | Promise<void>>;
  errorHandler?: DFWriteTransactionErrorHandler<DFUpdateOperation>;
} & DFOptionalConditionExpression;

export type DFDeleteOperation = {
  type: "Delete";
  key: Record<string, DynamoValue>;
  errorHandler?: DFWriteTransactionErrorHandler<DFDeleteOperation>;
} & DFOptionalConditionExpression;

export type DFConditionCheckOperation = {
  type: "ConditionCheck";
  key: Record<string, DynamoValue>;
  errorHandler?: DFWriteTransactionErrorHandler<DFConditionCheckOperation>;
} & DFConditionExpression;

export type DFWriteOperation =
  | DFUpdateOperation
  | DFDeleteOperation
  | DFConditionCheckOperation;
export type DFWritePrimaryOperation = DFUpdateOperation | DFDeleteOperation;
export type DFWriteSecondaryOperation =
  | DFUpdateOperation
  | DFDeleteOperation
  | DFConditionCheckOperation;
