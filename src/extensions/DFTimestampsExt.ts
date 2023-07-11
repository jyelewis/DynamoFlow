import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";

export class DFTimestampsExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(
    public readonly config: {
      createdAt?: keyof Entity;
      updatedAt?: keyof Entity;
    }
  ) {
    super();
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    const now = new Date().toISOString();

    if (this.config.createdAt) {
      transaction.primaryUpdateOperation.updateValues[
        this.config.createdAt as string
      ] = {
        $setIfNotExists: now,
      };
    }

    if (this.config.updatedAt) {
      transaction.primaryUpdateOperation.updateValues[
        this.config.updatedAt as string
      ] = {
        $setIfNotExists: now,
      };
    }
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    // ensure no one can update createdAt, if we're in control of the field
    if (this.config.createdAt) {
      entityUpdate[this.config.createdAt as string] = undefined;
    }

    if (this.config.updatedAt) {
      entityUpdate[this.config.updatedAt as string] = new Date().toISOString();
    }
  }
}
