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
    entity: EntityWithMetadata<Entity>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    entity[this.fieldName] = new Date().toISOString() as any;
  }

  public onUpdate(
    key: Partial<Entity>,
    partialEntity: EntityWithMetadata<
      Partial<Record<keyof Entity, UpdateValue>>
    >,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    partialEntity[this.fieldName] = new Date().toISOString() as any;
  }
}
