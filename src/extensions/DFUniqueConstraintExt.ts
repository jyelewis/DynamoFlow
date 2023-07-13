import {
  EntityWithMetadata,
  RETRY_TRANSACTION,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { isDynamoValue } from "../utils/isDynamoValue.js";
import { DFCollection } from "../DFCollection.js";
import { DFConditionalCheckFailedError } from "../errors/DFConditionalCheckFailedError.js";

// TODO: test

export class DFUniqueConstraintConflictError extends Error {
  constructor(uniqueField: string) {
    super(`Unique constraint violation on field '${uniqueField}'`);
  }
}

interface UniqueConstraintItem {
  val: string | number;
}

export class DFUniqueConstraintExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  private uniqueConstraintCollection!: DFCollection<UniqueConstraintItem>;
  public constructor(public readonly uniqueField: keyof Entity) {
    super();
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // a little meta, use a side collection for each unique constraint
    // create directly, rather than by the table so we have control over the prefix
    // and it won't get registered on the users table
    this.uniqueConstraintCollection = new DFCollection<UniqueConstraintItem>(
      this.collection.table,
      {
        name: `${collection.config.name}_unique_${this.uniqueField as string}`,
        partitionKey: "val",
      }
    );
  }

  public onInsert(entity: EntityWithMetadata, transaction: DFWriteTransaction) {
    const uniqueFieldValue = entity[this.uniqueField as string];
    if (uniqueFieldValue === null) {
      // no unique value to enforce
      return;
    }

    if (
      typeof uniqueFieldValue !== "string" &&
      typeof uniqueFieldValue !== "number"
    ) {
      throw new Error(
        `Field '${
          this.uniqueField as string
        }' can only be a string or number due to DFUniqueConstraintExt`
      );
    }

    // this will only succeed if the unique value doesn't already exist
    const uniqueConstraintTransaction =
      this.uniqueConstraintCollection.insertTransaction({
        val: uniqueFieldValue,
      });

    // custom error handlers to detect unique constraint violations
    uniqueConstraintTransaction.primaryOperation.errorHandler = (err: any) => {
      if (err instanceof DFConditionalCheckFailedError) {
        throw new DFUniqueConstraintConflictError(this.uniqueField as string);
      }

      throw err;
    };

    transaction.addSecondaryTransaction(uniqueConstraintTransaction);
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ) {
    const uniqueFieldValue = entityUpdate[this.uniqueField as string];
    if (uniqueFieldValue === undefined) {
      // field hasn't been updated, no need to change anything
      return;
    }

    if (!isDynamoValue(uniqueFieldValue)) {
      throw new Error(
        `Field '${
          this.uniqueField as string
        }' cannot be updated with dynamic values due to DFUniqueConstraintExt`
      );
    }

    if (
      uniqueFieldValue !== null &&
      typeof uniqueFieldValue !== "string" &&
      typeof uniqueFieldValue !== "number"
    ) {
      throw new Error(
        `Field '${
          this.uniqueField as string
        }' can only be a string, number or null due to DFUniqueConstraintExt`
      );
    }

    transaction.addPreCommitHandler(async () => {
      const existingItem: any = this.collection.retrieveOne({
        where: transaction.primaryUpdateOperation.key as any,
        returnRaw: true,
      });

      const oldUniqueFieldValue = existingItem[this.uniqueField as any];
      if (uniqueFieldValue === oldUniqueFieldValue) {
        // no change to unique value
        return;
      }

      if (oldUniqueFieldValue !== undefined && oldUniqueFieldValue !== null) {
        // we have an old unique value to delete
        transaction.addSecondaryTransaction(
          this.uniqueConstraintCollection.deleteTransaction({
            val: oldUniqueFieldValue,
          })
        );

        // fail our write if the item is updated between our read & write
        // if so we'll request a re-try of the transaction
        transaction.primaryUpdateOperation.condition =
          transaction.primaryUpdateOperation.condition || {};
        transaction.primaryUpdateOperation.condition._wc = existingItem._wc;
        transaction.primaryUpdateOperation.errorHandler = (err) => {
          if (err instanceof DFConditionalCheckFailedError) {
            return RETRY_TRANSACTION;
          }

          throw err;
        };
      }

      if (uniqueFieldValue === null) {
        // no unique value to store
      }

      // TODO: this is very duplicated code
      // store a new unique value
      const newUniqueConstraintTransaction =
        this.uniqueConstraintCollection.insertTransaction({
          val: uniqueFieldValue as any,
        });
      newUniqueConstraintTransaction.primaryOperation.errorHandler = (
        err: any
      ) => {
        if (err instanceof DFConditionalCheckFailedError) {
          throw new DFUniqueConstraintConflictError(this.uniqueField as string);
        }

        throw err;
      };
      transaction.addSecondaryTransaction(newUniqueConstraintTransaction);
    });
  }

  public onDelete(key: Partial<Entity>, transaction: DFWriteTransaction) {
    // TODO: complete this
    // TODO: need to fetch the item first
    // this.uniqueConstraintCollection.deleteTransaction({
    //   val: key
    // })
  }

  public migrateEntity(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    const uniqueFieldValue = entity[this.uniqueField as string];
    if (uniqueFieldValue === undefined || uniqueFieldValue === null) {
      // field doesn't exist or is null, no need to store any items in unique index
      return;
    }

    // store an item in our unique constraint collection (if it doesn't already exist)
    transaction.addSecondaryTransaction(
      this.uniqueConstraintCollection.insertTransaction(
        {
          val: uniqueFieldValue as any,
        },
        {
          allowOverwrite: true,
        }
      )
    );
  }

  public async valueExists(value: string | number): Promise<boolean> {
    const item = await this.uniqueConstraintCollection.retrieveOne({
      where: { val: value },
      returnRaw: true,
    });

    return !!item;
  }
}
