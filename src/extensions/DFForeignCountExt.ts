/* istanbul ignore file */
// TODO: complete me, this is a WIP

import {
  DynamoItem,
  EntityWithMetadata,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFCollection } from "../DFCollection.js";
import { isDynamoValue } from "../utils/isDynamoValue.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";

// TODO: test me
// TODO: support migrations
// TODO: can we write a helper, or build optimistic locking into DFCollection instead? We're doing the same thing here again

export interface DFForeignCountExtConfig<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> {
  // TODO: circular references (what if I also want a cascade delete here? Which extension gets defined first?)
  //       maybe this could accept a function that returns the collection on demand
  foreignCollection: DFCollection<ForeignEntity>;
  countField: keyof Entity;
  foreignEntityToLocalKey: [
    (foreignEntity: ForeignEntity) => null | Partial<Entity>,
    Array<keyof ForeignEntity>
  ];
  // queryForForeignEntities: (localEntity: Entity) => Query<ForeignEntity>;
}

export class DFForeignCountExt<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> extends DFBaseExtension<Entity> {
  public constructor(
    public readonly config: DFForeignCountExtConfig<Entity, ForeignEntity>
  ) {
    super();
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // install a remote extension to monitor for changes against foreign items
    // TODO: risk double initing here, is that an issue?
    const foreignCollectionMonitoringExtension =
      new DFInternalForeignCountRemoteExt(this.config, this.collection);
    foreignCollectionMonitoringExtension.init(this.config.foreignCollection);

    this.config.foreignCollection.extensions.push(
      foreignCollectionMonitoringExtension
    );
  }
}

// gets installed into the foreign collection
// allowing us to monitor updates & update the local collection
class DFInternalForeignCountRemoteExt<
  Entity extends SafeEntity<Entity>,
  ForeignEntity extends SafeEntity<ForeignEntity>
> extends DFBaseExtension<ForeignEntity> {
  public constructor(
    public readonly config: DFForeignCountExtConfig<Entity, ForeignEntity>,
    public readonly countingCollection: DFCollection<Entity>
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
      const existingItem = await this.config.foreignCollection.retrieveOne({
        where: key,
        returnRaw: true,
      });
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

      // decrement the count from oldLocalKey
      // TODO: aaah what if we have multiple counts causing multiple updates to the same item in a transaction...
      //       could we move this merge logic into addSecondaryTransaction...? (write a test for this and fix in DFWriteTransaction)
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
      const existingItem = await this.config.foreignCollection.retrieveOne({
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
}
