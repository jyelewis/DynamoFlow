import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";

export class DFTimestampsExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(
    public readonly config: {
      createdAtField?: keyof Entity;
      updatedAtField?: keyof Entity;
    }
  ) {
    super();
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    const now = new Date().toISOString();

    if (this.config.createdAtField) {
      transaction.primaryUpdateOperation.updateValues[
        this.config.createdAtField as string
      ] = {
        $setIfNotExists: now,
      };
    }

    if (this.config.updatedAtField) {
      transaction.primaryUpdateOperation.updateValues[
        this.config.updatedAtField as string
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
    if (this.config.createdAtField) {
      entityUpdate[this.config.createdAtField as string] = undefined;
    }

    if (this.config.updatedAtField) {
      entityUpdate[this.config.updatedAtField as string] =
        new Date().toISOString();
    }
  }
}
