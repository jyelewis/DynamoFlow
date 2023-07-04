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
import { isDynamoValue } from "../utils/isDynamoValue.js";
import { DFConditionalCheckFailedException } from "../errors/DFConditionalCheckFailedException.js";
import { ensureArray } from "../utils/ensureArray.js";

// TODO: not ready for production use, needs more testing

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
    protected readonly config: DFSecondaryIndexExtConfig<Entity>
  ) {
    super();

    this.pkKeys = ensureArray(this.config.partitionKey);
    this.skKeys = ensureArray(this.config.sortKey);

    this.includeInIndexKeys = this.config.includeInIndex
      ? (this.config.includeInIndex[1] as string[])
      : [];
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // ensure this DB supports GSIs
    if (!this.collection.table.config.GSIs) {
      throw new Error(`DB does not have any GSIs defined`);
    }

    // validate the index being used here exists on the database
    if (!this.collection.table.config.GSIs.includes(this.config.dynamoIndex)) {
      throw new Error(
        `GSI '${this.config.dynamoIndex}' not defined for this DB`
      );
    }

    const dynamoIndexAlreadyUsed = this.collection.extensions
      // find any other GSI extensions
      .filter((ext) => ext instanceof DFSecondaryIndexExt && ext !== this)
      // check if they are using the same index as us
      .some(
        (ext) =>
          (ext as DFSecondaryIndexExt<any>).config.dynamoIndex ===
          this.config.dynamoIndex
      );

    if (dynamoIndexAlreadyUsed) {
      throw new Error(
        `'${this.config.dynamoIndex}' already used by another index on collection ${this.collection.config.name}`
      );
    }
  }

  public onInsert(
    entity: EntityWithMetadata,
    _transaction: DFWriteTransaction
  ): void {
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

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void {
    // collect all the properties we can
    // we'll use these to compute the new GSI keys if needed & possible
    // if needed & not possible, we'll fetch the entity from Dynamo first to get all existing fields
    let entityWithSomeProperties = {
      ...key,
      ...entityUpdate,
    } as EntityWithMetadata;

    [...this.pkKeys, ...this.skKeys, ...this.includeInIndexKeys].forEach(
      (key) => {
        if (
          key in entityWithSomeProperties &&
          !isDynamoValue(entityWithSomeProperties[key])
        ) {
          // we could support this if needed
          // we'd just need to implement emulateDDBUpdate() ourselves, with all possible updates
          throw new Error(
            `Secondary index key '${key}' cannot accept dynamic updates`
          );
        }
      }
    );

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
      !(key in entityWithSomeProperties);

    const mustPreFetchEntity =
      this.pkKeys.some(requiredValueMissing) ||
      this.skKeys.some(requiredValueMissing) ||
      this.includeInIndexKeys.some(requiredValueMissing);

    // the rest of the logic here goes inside a preCommit handler
    // we will re-try from here if the transaction fails
    transaction.addPreCommitHandler(async () => {
      // on re-try this will always be true
      if (mustPreFetchEntity) {
        // TODO: auto batching / request deduping
        // TODO: we need the raw metadata which is being stripped here..
        const existingItem = (await this.collection.retrieveOne({
          where: key,
          returnRaw: true,
        })) as EntityWithMetadata;
        if (existingItem === null) {
          throw new Error(
            `Could not find entity with key ${JSON.stringify(key)}`
          );
        }

        // because we are reading before write, we need to add a condition expression
        // this allows us to optimistic lock the entity against our preFetched item
        // if someone updates this item between our read & write,
        // the write will fail and entire operation be re-tried
        /* istanbul ignore next */
        const primaryCondition = primaryUpdateOperation.condition || {};
        primaryUpdateOperation.condition = primaryCondition;

        primaryCondition._wc = {
          $eq: existingItem["_wc"],
        };

        // apply any changes from the update before we generate the new GSI keys
        entityWithSomeProperties = {
          ...existingItem,
          ...entityUpdate,
        } as EntityWithMetadata;

        // TODO: umm one error handler?
        primaryUpdateOperation.errorHandler = (err) => {
          // if our conditional check failed (likely due to a writeCount mismatch) re-try the transaction
          // this will allow is to re-fetch the entity and re-try the update
          if (err instanceof DFConditionalCheckFailedException) {
            return RETRY_TRANSACTION;
          }

          /* istanbul ignore next */
          throw err;
        };
      }

      // at this point we can be confident that entityWithRequiredProperties has all the properties we need
      // either we already had them, or we fetched them from Dynamo
      const entityWithRequiredProperties = entityWithSomeProperties;

      // once we have all the data we do our migration function can compute required properties
      this.migrateEntity(entityWithRequiredProperties, transaction);
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

  // TODO: test me
  public entityRequiresMigration(entity: EntityWithMetadata): boolean {
    const shouldBeIncludedInIndex =
      !this.config.includeInIndex ||
      this.config.includeInIndex[0](entity as Entity);

    const isIncludedInIndex = entity[this.indexPartitionKey] !== undefined;

    if (shouldBeIncludedInIndex === false && isIncludedInIndex === false) {
      // not included in index, no need to migrate
      return false;
    }

    if (shouldBeIncludedInIndex !== isIncludedInIndex) {
      // included in index mismatch
      return true;
    }

    if (
      entity[this.indexPartitionKey] !==
      `${this.collection.config.name}#${generateKeyString(this.pkKeys, entity)}`
    ) {
      // partition key mismatch
      return true;
    }

    if (
      entity[this.indexSortKey] !==
      `${this.collection.config.name}#${generateKeyString(this.skKeys, entity)}`
    ) {
      // sort key mismatch
      return true;
    }

    return false;
  }

  public migrateEntity(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction
  ) {
    // call our includeInIndex function if provided
    const includeInIndex =
      !this.config.includeInIndex ||
      this.config.includeInIndex[0](entity as Entity);

    // update the GSI keys
    if (includeInIndex) {
      // compute our new keys & write
      transaction.primaryUpdateOperation.updateValues[this.indexPartitionKey] =
        `${this.collection.config.name}#` +
        generateKeyString(this.pkKeys, entity);
      transaction.primaryUpdateOperation.updateValues[this.indexSortKey] =
        `${this.collection.config.name}#` +
        generateKeyString(this.skKeys, entity);
    } else {
      // removing the key values on our GSI will remove this item from the index
      transaction.primaryUpdateOperation.updateValues[this.indexPartitionKey] =
        {
          $remove: true,
        };
      transaction.primaryUpdateOperation.updateValues[this.indexSortKey] = {
        $remove: true,
      };
    }
  }

  private get indexPartitionKey(): string {
    return `_${this.config.dynamoIndex}PK`;
  }

  private get indexSortKey(): string {
    return `_${this.config.dynamoIndex}SK`;
  }
}
