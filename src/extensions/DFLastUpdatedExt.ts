import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";

// TODO: this is very much just a POC, re-write when appropriate

export class DFLastUpdatedExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(public readonly fieldName: keyof Entity) {
    super();
  }

  public onInsert(
    entity: EntityWithMetadata,
    _transaction: DFWriteTransaction
  ): void | Promise<void> {
    // TODO: kinda want to only write this if the item didn't already exist?
    // no clean way to write this expression with our system
    entity[this.fieldName as string] = new Date().toISOString();
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    entityUpdate[this.fieldName as string] = new Date().toISOString();
  }
}
