/* istanbul ignore file */
// TODO: complete me, this is a WIP

import {
  DynamoItem,
  EntityWithMetadata,
  Query,
  RETRY_TRANSACTION,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFCollection } from "../DFCollection.js";
import { isDynamoValue } from "../utils/isDynamoValue.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { DFConditionalCheckFailedError } from "../errors/DFConditionalCheckFailedError.js";

// TODO: support migrations

export interface DFForeignCountExtConfig<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> {
  foreignCollection:
    | DFCollection<ForeignEntity>
    // can optionally pass a getter function, to allow circular references between collections
    | (() => DFCollection<ForeignEntity>);
  countField: keyof Entity;
  foreignEntityToLocalKey: [
    (foreignEntity: ForeignEntity) => null | Partial<Entity>,
    Array<keyof ForeignEntity>
  ];
  queryForForeignEntities?: (localEntity: Entity) => Query<ForeignEntity>;
}

export class DFForeignCountExt<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> extends DFBaseExtension<Entity> {
  // lazy loaded in init(), to allow circular references between collections to be resolved
  public foreignCollection!: DFCollection<ForeignEntity>;
  public remoteExtension!: DFInternalForeignCountRemoteExt<
    Entity,
    ForeignEntity
  >;

  public constructor(
    public readonly config: DFForeignCountExtConfig<Entity, ForeignEntity>
  ) {
    super();
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    this.foreignCollection =
      typeof this.config.foreignCollection === "function"
        ? this.config.foreignCollection()
        : this.config.foreignCollection;

    // install a remote extension to monitor for changes against foreign items
    this.remoteExtension = new DFInternalForeignCountRemoteExt(
      this.config,
      this.collection,
      this.foreignCollection
    );

    this.remoteExtension.init(this.foreignCollection);

    this.foreignCollection.extensions.push(this.remoteExtension);
  }
}

// gets installed into the foreign collection
// allowing us to monitor updates & update the local collection
class DFInternalForeignCountRemoteExt<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> extends DFBaseExtension<ForeignEntity> {
  public maxMigrationQueryPages = 15;
  public constructor(
    public readonly config: DFForeignCountExtConfig<Entity, ForeignEntity>,
    public readonly countingCollection: DFCollection<Entity>,
    public readonly foreignCollection: DFCollection<ForeignEntity>
  ) {
    super();
  }

  public onInsert(
    foreignEntity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ) {
    const localKey = this.config.foreignEntityToLocalKey[0](
      foreignEntity as ForeignEntity
    );

    if (localKey === null) {
      // nothing to count!
      return;
    }

    transaction.addSecondaryTransaction(
      this.countingCollection.updateTransaction(localKey, {
        // one more matching item
        [this.config.countField]: { $inc: 1 },
      } as any)
    );
  }

  public onUpdate(
    key: Partial<ForeignEntity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ) {
    const countDependentFields = this.config.foreignEntityToLocalKey[1];
    const hasSomeFields = countDependentFields.some(
      (key) => key in entityUpdate
    );
    if (!hasSomeFields) {
      // dependent fields haven't changed, no need to update anything
      return;
    }

    let entityWithSomeProperties = { ...key, ...entityUpdate } as DynamoItem;

    // check that no dynamic expressions have been used
    // for fields we depend on to compute the counting item ID
    countDependentFields.forEach((key) => {
      if (!isDynamoValue(entityWithSomeProperties[key])) {
        throw new Error(
          `Cannot use dynamic update expression for key ${
            key as string
          } because ${this.countingCollection.config.name}.${
            this.config.countField as string
          } requires a literal value to maintain counts`
        );
      }
    });

    transaction.addPreCommitHandler(async () => {
      const existingItem = (await this.foreignCollection.retrieveOne({
        where: key,
        returnRaw: true,
      })) as EntityWithMetadata;
      if (existingItem === null) {
        throw new Error("Entity does not exist");
      }

      // apply any changes from the update before we check the new foreign ID
      entityWithSomeProperties = {
        ...existingItem,
        ...entityWithSomeProperties,
      } as EntityWithMetadata;

      // we know at this point that we have all required keys
      const entityWithRequiredProperties = entityWithSomeProperties;

      const oldLocalKey = this.config.foreignEntityToLocalKey[0](
        existingItem as ForeignEntity
      );
      const newLocalKey = this.config.foreignEntityToLocalKey[0](
        entityWithRequiredProperties as ForeignEntity
      );

      if (oldLocalKey === newLocalKey) {
        // no counts to update, bail
        return;
      }

      // TODO: test this
      const primaryCondition =
        transaction.primaryUpdateOperation.condition || {};
      primaryCondition._wc = existingItem._wc;

      transaction.primaryOperation.errorHandler = (err: any) => {
        // TODO: test this
        if (err instanceof DFConditionalCheckFailedError) {
          return RETRY_TRANSACTION;
        }

        /* istanbul ignore next */
        throw err;
      };

      // decrement the count from oldLocalKey
      if (oldLocalKey !== null) {
        transaction.addSecondaryTransaction(
          this.countingCollection.updateTransaction(oldLocalKey, {
            [this.config.countField]: { $inc: -1 },
          } as any)
        );
      }

      // increment the count for newLocalKey
      if (newLocalKey !== null) {
        transaction.addSecondaryTransaction(
          this.countingCollection.updateTransaction(newLocalKey, {
            [this.config.countField]: { $inc: 1 },
          } as any)
        );
      }
    });
  }

  public onDelete(
    key: Partial<ForeignEntity>,
    transaction: DFWriteTransaction
  ) {
    transaction.addPreCommitHandler(async () => {
      const existingItem = await this.foreignCollection.retrieveOne({
        where: key,
        returnRaw: true,
      });
      if (existingItem === null) {
        // nothing to do, bail
        return;
      }

      const oldLocalKey = this.config.foreignEntityToLocalKey[0](
        existingItem as ForeignEntity
      );

      if (oldLocalKey === null) {
        // no items reference this
        // no count to decrement
        return;
      }

      transaction.addSecondaryTransaction(
        this.countingCollection.updateTransaction(oldLocalKey, {
          [this.config.countField]: { $inc: -1 },
        } as any)
      );
    });
  }

  // TODO: test me
  public async migrateEntity(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ): Promise<void> {
    if (this.config.queryForForeignEntities === undefined) {
      // cannot migrate without queryForForeignEntities provided
      return;
    }

    const foreignQuery = this.config.queryForForeignEntities(entity as Entity);
    // save some processing, no need to process the actual items
    // would be nice to have a way to not return any fields, we don't need the data
    foreignQuery.returnRaw = true;

    let numberOfForeignItems = 0;
    let completedQuery = false;

    for (let i = 0; i < this.maxMigrationQueryPages; i++) {
      const { items, lastEvaluatedKey } =
        await this.foreignCollection.retrieveManyWithPagination(foreignQuery);

      numberOfForeignItems += items.length;
      foreignQuery.exclusiveStartKey = lastEvaluatedKey;

      if (lastEvaluatedKey === null) {
        completedQuery = true;
        break;
      }
    }

    if (!completedQuery) {
      console.warn(
        "Unable to re-compute foreign count, too many pages of foreign items"
      );
      return;
    }

    if (entity[this.config.countField as string] !== numberOfForeignItems) {
      transaction.primaryUpdateOperation.updateValues[
        this.config.countField as string
      ] = numberOfForeignItems;
    }
  }
}
