import { DFDB } from "./DFDB.js";
import { UpdateCommandInput, DeleteCommandInput } from "@aws-sdk/lib-dynamodb";
import {
  CancellationReason,
  TransactionCanceledException,
} from "@aws-sdk/client-dynamodb";
import { WriteTransactionFailedError } from "./errors/WriteTransactionFailedError.js";
import { DynamoItem, DynamoValue, RETRY_TRANSACTION } from "./types/types.js";
import {
  DFConditionCheckOperation,
  DFDeleteOperation,
  DFUpdateOperation,
  DFWritePrimaryOperation,
  DFWriteSecondaryOperation,
} from "./types/operations.js";
import assert from "assert";

const MAX_TRANSACTION_RETRIES = 5;

// have to re-declare as the dynamo client doesn't export this type for us to build upon
interface ConditionCheckCommandInput {
  Key: Record<string, DynamoValue>;
  TableName: string;
  ConditionExpression: string;
  ExpressionAttributeNames: Record<string, string>;
  ExpressionAttributeValues?: Record<string, DynamoValue>;
}

export class DFWriteTransaction {
  // potential future problem
  // a single key cannot have multiple transaction items operate against it at once
  // could have to merge ops in future if they interact with the same item...
  // Maybe that's a cleaner way to add meta properties to objects anyway though? idk
  private retryCount = 0;
  public readonly secondaryOperations: DFWriteSecondaryOperation[] = [];
  public readonly preCommitHandlers: Array<() => Promise<void>> = [];
  public resultTransformer?: (
    item: DynamoItem
  ) => Promise<DynamoItem> | DynamoItem;

  public constructor(
    public db: DFDB,
    public primaryOperation: DFWritePrimaryOperation
  ) {}

  public addSecondaryOperation(op: DFWriteSecondaryOperation) {
    // or even take a callback to handle an error
    this.secondaryOperations.push(op);
  }

  public addSecondaryTransaction(secondaryTransaction: DFWriteTransaction) {
    this.secondaryOperations.push(secondaryTransaction.primaryOperation);
    this.secondaryOperations.push(...secondaryTransaction.secondaryOperations);
    // TODO: test me
    this.preCommitHandlers.push(...secondaryTransaction.preCommitHandlers);
    // leave their resultTransformer behind, only needed for the primary item
  }

  public addPreCommitHandler(handlerFn: () => Promise<void>) {
    // pre-commit handlers will run right before the commit
    // allowing read-before-write operations
    // if the commit fails and is re-tried, the pre-commit handler will be run again
    this.preCommitHandlers.push(handlerFn);
  }

  public async commit(): Promise<DynamoItem | null> {
    await Promise.all(this.preCommitHandlers.map((x) => x()));

    if (this.secondaryOperations.length === 0) {
      try {
        // executeSingle always returns the full item from primaryOperation
        // if we are doing a write operation
        // types were too annoying to express
        const item = await this.executeSingle();

        if (
          this.primaryOperation.type === "Update" &&
          this.primaryOperation.successHandlers
        ) {
          assert(item);
          await Promise.all(
            this.primaryOperation.successHandlers.map((handler) =>
              handler(item)
            )
          );
        }

        // call the provided transform function (if any) on the raw row before we return it
        return item && this.resultTransformer
          ? await this.resultTransformer(item)
          : item;
      } catch (e: any) {
        // make errors all look like CancellationReasons so we are consistent with multi-op error handling
        const cancellationReason: CancellationReason = {
          Code: e.name,
          Message: e.name,
        };

        if (!this.primaryOperation.errorHandler) {
          throw new WriteTransactionFailedError(e as Error);
        }

        const errorHandlerResponse = this.primaryOperation.errorHandler(
          cancellationReason,
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore: intentionally didn't want to type PrimaryOperation down to the op type - too much complexity
          this.primaryOperation
        );

        switch (errorHandlerResponse) {
          case RETRY_TRANSACTION: {
            if (this.retryCount >= MAX_TRANSACTION_RETRIES) {
              throw new WriteTransactionFailedError("Max retries exceeded");
            }
            this.retryCount += 1;

            return this.commit();
          }
          default:
            throw new Error(
              "Transaction operation error handler returned invalid response. Error handlers must throw an error themselves, or return RETRY_TRANSACTION"
            );
        }
      }
    }

    try {
      await this.executeMany();
    } catch (e: unknown) {
      /* istanbul ignore next */
      if (!(e instanceof TransactionCanceledException)) {
        throw e;
      }

      /* istanbul ignore next */
      if (!e.CancellationReasons) {
        throw new WriteTransactionFailedError(
          `Transaction failed, but no CancellationReasons were provided: ${e}`
        );
      }

      for (const [index, reason] of e.CancellationReasons.entries()) {
        if (reason.Code === "None") {
          continue; // no errors here
        }

        const op =
          index === 0
            ? this.primaryOperation
            : this.secondaryOperations[index - 1];

        if (op.errorHandler) {
          const errorHandlerResponse = op.errorHandler(e, op as any);

          switch (errorHandlerResponse) {
            case RETRY_TRANSACTION: {
              if (this.retryCount >= MAX_TRANSACTION_RETRIES) {
                throw new WriteTransactionFailedError("Max retries exceeded");
              }
              this.retryCount += 1;

              return this.commit();
            }
            default:
              throw new Error(
                "Transaction operation error handler returned invalid response. Error handlers must throw an error themselves, or return RETRY_TRANSACTION"
              );
          }
        }
      }

      // we'd generally expect an errorHandler to exist, but it's possible to add an operation that can fail with no handler
      throw new WriteTransactionFailedError(e);
    }

    // everything below here is to support onSuccess handlers & return value

    // kind of annoying, but we need to fetch the entity(s) after the transaction completes
    // transactions don't support returning the updated item
    const updateOpsNeededForFetch: Array<{
      index: number;
      handlers: Array<(item: DynamoItem) => void | Promise<void>>;
    }> = [];
    let primaryOperationReturnValue: DynamoItem | null = null;

    if (this.primaryOperation.type === "Update") {
      updateOpsNeededForFetch.push({
        index: 0,
        // we always need to fetch the primary operation back if it's a write
        // it's required for the return value of this function
        handlers: this.primaryOperation.successHandlers || [],
      });
    }

    for (const [index, op] of this.secondaryOperations.entries()) {
      if (
        op.type === "Update" &&
        op.successHandlers !== undefined &&
        op.successHandlers.length > 0
      ) {
        updateOpsNeededForFetch.push({
          index: index + 1,
          handlers: op.successHandlers,
        });
      }
    }

    await Promise.all(
      updateOpsNeededForFetch.map(async ({ index, handlers }) => {
        const op =
          index === 0
            ? this.primaryOperation
            : this.secondaryOperations[index - 1];

        const res = await this.db.client.get({
          TableName: this.db.tableName,
          Key: op.key,
          // want to grab a consistent read so we are sure to read at least past our write
          ConsistentRead: true,
        });

        /* istanbul ignore next */
        if (res.Item === undefined) {
          // must have been deleted between write and read?
          // not much we can do here, and success handlers aren't 'guaranteed' to be called
          console.warn(
            "Unable to call transaction success handler, item deleted"
          );
          return;
        }

        if (index === 0) {
          // this is the primary item, keep a copy of the value so we can return it later
          primaryOperationReturnValue = res.Item as DynamoItem;
        }

        await Promise.all(handlers.map((handler) => handler(res.Item as any)));
      })
    );

    // call the provided transform function (if any) on the raw row before we return it
    return primaryOperationReturnValue && this.resultTransformer
      ? await this.resultTransformer(primaryOperationReturnValue)
      : primaryOperationReturnValue;
  }

  public async commitWithReturn(): Promise<DynamoItem> {
    // typescript wrapper
    if (this.primaryOperation.type !== "Update") {
      throw new Error(
        "Cannot call commitWithReturn() on a transaction with no primary operation of type 'Update'"
      );
    }

    return (await this.commit()) as DynamoItem;
  }

  private async executeSingle(): Promise<DynamoItem | null> {
    const op = this.primaryOperation;

    switch (op.type) {
      case "Update": {
        const updateRes = await this.db.client.update(
          this.updateExpressionToParams(op)
        );
        return updateRes.Attributes as DynamoItem;
      }
      case "Delete": {
        await this.db.client.delete(this.deleteExpressionToParams(op));
        return null;
      }
      default:
        throw new Error(`Unknown operation type`);
    }
  }

  private async executeMany(): Promise<void> {
    const ops = [this.primaryOperation, ...this.secondaryOperations];
    const transactionItems: any[] = [];
    for (const op of ops) {
      switch (op.type) {
        case "Update":
          transactionItems.push({
            Update: this.updateExpressionToParams(op),
          });
          continue;
        case "Delete":
          transactionItems.push({
            Delete: this.deleteExpressionToParams(op),
          });
          continue;
        case "ConditionCheck":
          transactionItems.push({
            ConditionCheck: this.conditionCheckExpressionToParams(op),
          });
          continue;
        default:
          throw new Error(`Unknown operation type`);
      }
    }

    await this.db.client.transactWrite({
      TransactItems: transactionItems,
    });
  }

  private updateExpressionToParams(op: DFUpdateOperation): UpdateCommandInput {
    // load in the condition expression values, if we have any
    const expressionAttributeNames: Record<string, any> =
      "conditionExpression" in op
        ? { ...op.conditionExpressionAttributeNames }
        : {};
    const expressionAttributeValues: Record<string, any> =
      "conditionExpression" in op
        ? { ...op.conditionExpressionAttributeValues }
        : {};

    // generate an update expression & add the values to the expressionAttributes
    const operations: { SET: string[]; REMOVE: string[] } = {
      SET: [],
      REMOVE: [],
    };
    Object.keys(op.updateValues).forEach((key, index) => {
      expressionAttributeNames[`#update_key${index}`] = key;

      const updateValue = op.updateValues[key];
      if (typeof updateValue === "object" && updateValue !== null) {
        if ("$inc" in updateValue) {
          // "SET #age = if_not_exists(#age, :zero) + :inc"
          expressionAttributeValues[`:update_value${index}`] =
            updateValue["$inc"];
          expressionAttributeValues[`:zero`] = 0;

          operations.SET.push(
            `#update_key${index}=if_not_exists(#update_key${index}, :zero) + :update_value${index}`
          );
          return;
        }

        if ("$remove" in updateValue) {
          // "REMOVE #age"
          operations.REMOVE.push(`#update_key${index}`);
          return;
        }
      }

      expressionAttributeValues[`:update_value${index}`] = op.updateValues[key];

      operations.SET.push(`#update_key${index}=:update_value${index}`);
    });

    const updateExpressions = [];
    if (operations.SET.length > 0) {
      updateExpressions.push(`SET ${operations.SET.join(", ")}`);
    }
    if (operations.REMOVE.length > 0) {
      updateExpressions.push(`REMOVE ${operations.REMOVE.join(", ")}`);
    }
    const fullUpdateExpression = updateExpressions.join(" ");

    return {
      TableName: this.db.tableName,
      Key: op.key,
      UpdateExpression: fullUpdateExpression,
      ConditionExpression:
        "conditionExpression" in op ? op.conditionExpression : undefined,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: "ALL_NEW",
    };
  }

  private deleteExpressionToParams(op: DFDeleteOperation): DeleteCommandInput {
    return {
      TableName: this.db.tableName,
      Key: op.key,
      ConditionExpression:
        "conditionExpression" in op ? op.conditionExpression : undefined,
      ExpressionAttributeNames:
        "conditionExpression" in op
          ? op.conditionExpressionAttributeNames
          : undefined,
      ExpressionAttributeValues:
        "conditionExpression" in op
          ? op.conditionExpressionAttributeValues
          : undefined,
    };
  }

  private conditionCheckExpressionToParams(
    op: DFConditionCheckOperation
  ): ConditionCheckCommandInput {
    return {
      TableName: this.db.tableName,
      Key: op.key,
      ConditionExpression: op.conditionExpression,
      ExpressionAttributeNames: op.conditionExpressionAttributeNames,
      ExpressionAttributeValues: op.conditionExpressionAttributeValues,
    } as any;
  }
}
