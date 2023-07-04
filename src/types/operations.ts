import { DynamoItem, DynamoValue, UpdateValue } from "./types.js";
import { CancellationReason } from "@aws-sdk/client-dynamodb";
import { DFConditionalCheckFailedException } from "../errors/DFConditionalCheckFailedException.js";
import { DFWriteTransactionFailedError } from "../errors/DFWriteTransactionFailedError.js";

export type DFConditionValue =
  | DynamoValue
  | {
      $eq: DynamoValue;
    }
  | {
      $ne: DynamoValue;
    }
  | {
      $exists: boolean;
    }
  | {
      $gt: DynamoValue;
    }
  | {
      $gte: DynamoValue;
    }
  | {
      $lt: DynamoValue;
    }
  | {
      $lte: DynamoValue;
    }
  | {
      $beginsWith: DynamoValue;
    }
  | {
      $in: DynamoValue[];
    }
  | {
      $betweenIncl: [DynamoValue, DynamoValue];
    }
  | {
      $contains: DynamoValue;
    }
  | {
      // allow providing any query by literal fields
      $raw: Required<ConditionExpressionProperties>;
    };

export type DFCondition = Record<string, DFConditionValue>;

export interface ConditionExpressionProperties {
  conditionExpression?: string;
  expressionAttributeNames?: Record<string, string>;
  expressionAttributeValues?: Record<string, DynamoValue>;
}

export type DFWriteTransactionErrorHandler<Operation extends DFWriteOperation> =
  (
    error:
      | DFWriteTransactionFailedError
      | DFConditionalCheckFailedException
      | CancellationReason,
    op: Operation
  ) => symbol | Promise<symbol>;

export type DFUpdateOperation = {
  type: "Update";
  key: Record<string, DynamoValue>;
  updateValues: Record<string, UpdateValue>;
  condition?: DFCondition;
  successHandlers?: Array<(item: DynamoItem) => void | Promise<void>>;
  errorHandler?: DFWriteTransactionErrorHandler<DFUpdateOperation>;
};

export type DFDeleteOperation = {
  type: "Delete";
  key: Record<string, DynamoValue>;
  condition?: DFCondition;
  errorHandler?: DFWriteTransactionErrorHandler<DFDeleteOperation>;
};

export type DFConditionCheckOperation = {
  type: "ConditionCheck";
  key: Record<string, DynamoValue>;
  condition: DFCondition;
  errorHandler?: DFWriteTransactionErrorHandler<DFConditionCheckOperation>;
};

export type DFWriteOperation =
  | DFUpdateOperation
  | DFDeleteOperation
  | DFConditionCheckOperation;
export type DFWritePrimaryOperation = DFUpdateOperation | DFDeleteOperation;
export type DFWriteSecondaryOperation =
  | DFUpdateOperation
  | DFDeleteOperation
  | DFConditionCheckOperation;
