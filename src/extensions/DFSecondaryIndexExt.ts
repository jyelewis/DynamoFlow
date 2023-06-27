import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { generateKeyString } from "../utils/generateKeyString.js";
import {
  EntityWithMetadata,
  Query,
  RETRY_TRANSACTION,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { PartialQueryExpression } from "../types/internalTypes.js";
import { generateQueryExpression } from "../utils/generateQueryExpression.js";
import { DFCollection } from "../DFCollection.js";
import { DFUpdateOperation } from "../types/operations.js";

interface DFSecondaryIndexExtConfig<Entity extends SafeEntity<Entity>> {
  indexName: string;
  dynamoIndex: "GSI1" | "GSI2" | "GSI3" | "GSI4" | "GSI5";
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
  includeInIndex?: [(entity: Entity) => boolean, Array<keyof Entity>];
}

export class DFSecondaryIndexExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  private readonly pkKeys: string[];
  private readonly skKeys: string[];
  private readonly includeInIndexKeys: string[];

  public constructor(
    private readonly config: DFSecondaryIndexExtConfig<Entity>
  ) {
    super();

    this.pkKeys = Array.isArray(this.config.partitionKey)
      ? this.config.partitionKey
      : [this.config.partitionKey];
    if (this.config.sortKey) {
      this.skKeys = Array.isArray(this.config.sortKey)
        ? this.config.sortKey
        : [this.config.sortKey];
    } else {
      this.skKeys = [];
    }

    this.includeInIndexKeys = this.config.includeInIndex
      ? (this.config.includeInIndex[1] as string[])
      : [];
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // ensure this DB supports GSIs
    if (!this.collection.db.config.GSIs) {
      throw new Error(`DB does not have any GSIs defined`);
    }

    // validate the index being used here exists on the database
    if (!this.collection.db.config.GSIs.includes(this.config.dynamoIndex)) {
      throw new Error(
        `GSI '${this.config.dynamoIndex}' not defined for this DB`
      );
    }
  }

  public onInsert(
    entity: EntityWithMetadata,
    _transaction: DFWriteTransaction
  ): void | Promise<void> {
    if (
      this.config.includeInIndex &&
      !this.config.includeInIndex[0](entity as Entity)
    ) {
      // don't write to this secondary index
      // leave out keys, Dynamo will not write to the index if the keys are missing
      return;
    }

    entity[this.indexPartitionKey] =
      `${this.collection.config.name}#` +
      generateKeyString(this.pkKeys, entity);

    entity[this.indexSortKey] =
      `${this.collection.config.name}#` +
      generateKeyString(this.skKeys, entity);
  }

  public async onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): Promise<void> {
    // collect all the properties we can
    // we'll use these to compute the new GSI keys if needed & possible
    // if needed & not possible, we'll fetch the entity from Dynamo first to get all existing fields
    let entityWithSomeProperties = {
      ...key,
      ...entityUpdate,
    } as EntityWithMetadata;

    // janky API, but this allows us to append a condition expression to the update
    // validating no one has written to the entity since we read it
    // if they have, our transaction will be rejected and we can re-try
    const primaryUpdateOperation =
      transaction.primaryOperation as Required<DFUpdateOperation>;

    // boolean: 'key' has changed, and therefore requires we update the GSI
    // specifically, we check the entityUpdate - not the key, they key never changes
    const requiredValueChanged = (key: string) => key in entityUpdate;
    const mustUpdateGSI =
      this.pkKeys.some(requiredValueChanged) ||
      this.skKeys.some(requiredValueChanged) ||
      this.includeInIndexKeys.some(requiredValueChanged);

    if (!mustUpdateGSI) {
      // no need to update the GSI, we're done
      return;
    }

    // we need to update the GSI fields
    // check whether we already have the required fields to perform checks & generate keys
    // boolean: 'key' does not have a literal update value
    const requiredValueMissing = (key: string) =>
      !(key in entityWithSomeProperties) ||
      (typeof entityWithSomeProperties[key] === "object" &&
        entityWithSomeProperties[key] !== null);

    const mustPreFetchEntity =
      this.pkKeys.some(requiredValueMissing) ||
      this.skKeys.some(requiredValueMissing) ||
      this.includeInIndexKeys.some(requiredValueMissing);

    // the rest of the logic here goes inside a preCommit handler
    // we will re-try from here if the transaction fails
    transaction.addPreCommitHandler(async () => {
      // on re-try this will always be true
      if (mustPreFetchEntity) {
        // TODO: all of this could be provided by the DB and include auto batching / request deduping
        const existingItemResponse = await this.collection.db.client.get({
          TableName: this.collection.db.tableName,
          Key: key,
        });
        if (!existingItemResponse.Item) {
          throw new Error(
            `Could not find entity with key ${JSON.stringify(key)}`
          );
        }

        // TODO: aah! Must apply changes here first
        // convert our raw fetch into an entity
        entityWithSomeProperties =
          await this.collection.entityFromRawDynamoItem(
            existingItemResponse.Item
          );

        // because we are reading before write, we need to add a condition expression
        // this allows us to optimistic lock the entity against our preFetched item
        // if someone updates this item between our read & write,
        // the write will fail and entire operation be re-tried
        const primaryCondition = primaryUpdateOperation.condition || {};
        primaryUpdateOperation.condition = primaryCondition;

        primaryCondition._wc = {
          $eq: entityWithSomeProperties["_wc"],
        };

        // TODO: umm one error handler?
        primaryUpdateOperation.errorHandler = (err) => {
          // if our conditional check failed (likely due to a writeCount mismatch) re-try the transaction
          // this will allow is to re-fetch the entity and re-try the update
          if (err.Code === "ConditionalCheckFailedException") {
            return RETRY_TRANSACTION;
          }

          throw err;
        };
      }

      // at this point we can be confident that entityWithRequiredProperties has all the properties we need
      // either we already had them, or we fetched them from Dynamo
      const entityWithRequiredProperties = entityWithSomeProperties;

      // call our includeInIndex function if provided
      const includeInIndex =
        !this.config.includeInIndex ||
        this.config.includeInIndex[0](entityWithRequiredProperties as Entity);

      // update the GSI keys
      if (includeInIndex) {
        // compute our new keys & write
        primaryUpdateOperation.updateValues[this.indexPartitionKey] =
          `${this.collection.config.name}#` +
          generateKeyString(this.skKeys, entityWithRequiredProperties);
        primaryUpdateOperation.updateValues[this.indexPartitionKey] =
          `${this.collection.config.name}#` +
          generateKeyString(this.pkKeys, entityWithRequiredProperties);
      } else {
        // removing the key values on our GSI will remove this item from the index
        primaryUpdateOperation.updateValues[this.indexPartitionKey] = {
          $remove: true,
        };
        primaryUpdateOperation.updateValues[this.indexSortKey] = {
          $remove: true,
        };
      }
    });
  }

  public expressionForQuery(
    query: Query<Entity>
  ): undefined | PartialQueryExpression {
    if (query.index !== this.config.indexName) {
      // not for us
      return undefined;
    }

    const queryExpression = generateQueryExpression(
      // still prefix items with the collection name, including in GSIs
      this.collection.config.name,
      this.config.partitionKey,
      this.config.sortKey,
      query
    );

    // tell Dynamo to use the GSI rather than the primary table
    queryExpression.indexName = this.config.dynamoIndex;
    queryExpression.expressionAttributeNames["#PK"] = this.indexPartitionKey;
    queryExpression.expressionAttributeNames["#SK"] = this.indexSortKey;

    return queryExpression;
  }

  public postRetrieve(
    entity: Partial<EntityWithMetadata>
  ): void | Promise<void> {
    // TODO: validate our key and call should exist in index check
    // we may want to migrate the entity if things are corrupt
    // TODO: should this return a symbol so we can exclude items from the results?
  }

  private get indexPartitionKey(): string {
    return `_${this.config.dynamoIndex}PK`;
  }

  private get indexSortKey(): string {
    return `_${this.config.dynamoIndex}SK`;
  }
}
