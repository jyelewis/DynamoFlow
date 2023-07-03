import { DFBaseExtension } from "./DFBaseExtension.js";
import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";

export interface DFMigrationExtConfig<Entity extends SafeEntity<Entity>> {
  version: number;
  migrateEntity: (
    currentEntityVersion: number,
    entity: EntityWithMetadata
  ) =>
    | Partial<Record<keyof Entity, UpdateValue>>
    | Promise<Partial<Record<keyof Entity, UpdateValue>>>;
}

export class DFMigrationExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(public readonly config: DFMigrationExtConfig<Entity>) {
    super();
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    // store version of this item
    transaction.primaryUpdateOperation.updateValues._v = this.config.version;
  }

  public entityRequiresMigration(entity: EntityWithMetadata): boolean {
    const currentEntityVersion = (entity._v || 0) as number;

    return currentEntityVersion < this.config.version;
  }

  public async migrateEntity(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): Promise<void> {
    const currentEntityVersion = (entity._v || 0) as number;

    const migrationUpdates = await this.config.migrateEntity(
      currentEntityVersion,
      entity
    );

    // append any user requested updates to our migration transaction
    Object.assign(
      transaction.primaryUpdateOperation.updateValues,
      migrationUpdates,
      {
        // record updated version
        _v: this.config.version,
      }
    );
  }
}
