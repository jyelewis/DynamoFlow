import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";

// TODO: this is very much just a POC, re-write when appropriate
// TODO: prevent setting 'createdAt' or 'updatedAt'

export class DFLastUpdatedExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(public readonly fieldName: keyof Entity) {
    super();
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    transaction.primaryUpdateOperation.updateValues[this.fieldName as string] =
      {
        $setIfNotExists: new Date().toISOString(),
      };
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    entityUpdate[this.fieldName as string] = new Date().toISOString();
  }
}
