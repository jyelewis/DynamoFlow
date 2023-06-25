// hooks interface
// - onInsert
// - onUpdate
// - onDelete
// - expressionForQuery - provide a queryExpression if appropriate to handle custom indexes
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

export abstract class DFBaseExtension<Entity extends SafeEntity<Entity>> {
  private _collection: undefined | DFCollection<Entity>;
  protected get collection(): DFCollection<Entity> {
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
  ): void | Promise<void> {}

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {}

  public onDelete(
    key: Partial<Entity>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {}

  public expressionForQuery(
    query: Query<Entity>
  ): undefined | PartialQueryExpression | Promise<PartialQueryExpression> {
    return undefined;
  }

  public postRetrieve(
    entity: Partial<EntityWithMetadata>
  ): void | Promise<void> {}
}
