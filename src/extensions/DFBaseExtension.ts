// hooks interface
// - onInsert
// - onUpdate
// - onDelete
// - expressionForQuery - provide a queryExpression if appropriate to handle custom indexes
// - entityRequiresMigration
// - migrateEntity
// - postRetrieve (no pre available, also called on all other functions to check / transform object)

import { DFCollection } from "../DFCollection.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import {
  EntityWithMetadata,
  Query,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { PartialQueryExpression } from "../types/internalTypes.js";
import { DFCondition } from "../types/operations.js";

export abstract class DFBaseExtension<Entity extends SafeEntity<Entity>> {
  public _collection: undefined | DFCollection<Entity>;
  public get collection(): DFCollection<Entity> {
    if (!this._collection) {
      throw new Error("Collection not set, had this extension been init()'ed?");
    }
    return this._collection;
  }
  public init(collection: DFCollection<Entity>) {
    this._collection = collection;
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void {}

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void {}

  public onDelete(
    key: Partial<Entity>,
    transaction: DFWriteTransaction
  ): void {}

  public expressionForQuery(
    query: Query<Entity>
  ): undefined | PartialQueryExpression {
    return undefined;
  }

  // TODO: can expression for query be merged into onQuery? Could have a 'raw' property on the query that needs populating
  public onQuery(query: Query<Entity>): undefined | DFCondition {
    return undefined;
  }

  public entityRequiresMigration(entity: EntityWithMetadata): boolean {
    return false;
  }

  public migrateEntity(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): void | Promise<void> {}

  public postRetrieve(entity: EntityWithMetadata): void | Promise<void> {}
}
