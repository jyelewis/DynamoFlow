import { DFBaseExtension } from "./extensions/DFBaseExtension.js";
import { DFTable } from "./DFTable.js";
import { DFWriteTransaction } from "./DFWriteTransaction.js";
import {
  DynamoItem,
  EntityWithMetadata,
  Query,
  RETRY_TRANSACTION,
  SafeEntity,
  UpdateValue,
} from "./types/types.js";
import { generateIndexStrings } from "./utils/generateIndexStrings.js";
import { generateQueryExpression } from "./utils/generateQueryExpression.js";
import { DFCondition, DFWritePrimaryOperation } from "./types/operations.js";
import { conditionToConditionExpression } from "./utils/conditionToConditionExpression.js";
import { DFConditionalCheckFailedError } from "./errors/DFConditionalCheckFailedError.js";
import { ensureArray } from "./utils/ensureArray.js";

export interface DFCollectionConfig<Entity extends SafeEntity<Entity>> {
  name: string;
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
  extensions?: DFBaseExtension<Entity>[];
}

export class DFCollection<Entity extends SafeEntity<Entity>> {
  public extensions: DFBaseExtension<Entity>[];
  public readOnlyFields: Array<keyof Entity>;

  constructor(
    public readonly table: DFTable,
    public readonly config: DFCollectionConfig<Entity>
  ) {
    if (this.config.name in this.table.collections) {
      throw new Error(
        `Collection '${this.config.name}' already exists in this table`
      );
    }

    this.readOnlyFields = [
      ...ensureArray(this.config.partitionKey),
      ...ensureArray(this.config.sortKey),
    ];

    // init extensions
    this.extensions = config.extensions || [];
    this.extensions.forEach((extension) => extension.init(this));
  }

  public insertTransaction(
    newEntity: Entity,
    options?: {
      allowOverwrite?: boolean;
    }
  ): DFWriteTransaction {
    const entityWithMetadata: EntityWithMetadata = { ...newEntity };

    // used for table scans so we can call the appropriate collection to handle this entity
    entityWithMetadata["_c"] = this.config.name;
    // allows extensions to perform optimistic locking on entities
    // without storing extra metadata properties themselves
    entityWithMetadata["_wc"] = 1;

    const allowOverwrite = options && options.allowOverwrite;

    const [pk, sk] = generateIndexStrings(
      this.config.name,
      this.config.partitionKey,
      this.config.sortKey,
      entityWithMetadata
    );
    const transaction = this.table.createTransaction({
      type: "Update",
      key: {
        _PK: pk,
        _SK: sk,
      },
      updateValues: entityWithMetadata,
      condition: !allowOverwrite
        ? {
            // if not allowing overwrite, ensure this item doesn't already exist
            _PK: {
              $exists: false,
            },
          }
        : undefined,
      errorHandler: (e: any) => {
        if (e instanceof DFConditionalCheckFailedError) {
          throw new Error("Entity already exists");
        }

        /* istanbul ignore next */
        throw e;
      },
    });

    // run extensions
    this.extensions.map((extension) =>
      extension.onInsert(entityWithMetadata, transaction)
    );

    // gotta still run postRetrieves on writes
    transaction.resultTransformer = this.entityFromRawDynamoItem.bind(this);

    return transaction;
  }

  // convenience function
  public async insert(
    newEntity: Entity,
    options?: {
      allowOverwrite?: boolean;
    }
  ): Promise<Entity> {
    const transaction = this.insertTransaction(newEntity, options);
    return (await transaction.commit()) as Entity;
  }

  public updateTransaction(
    key: Partial<Entity>,
    updateFields: Partial<Record<keyof Entity, UpdateValue>>
  ): DFWriteTransaction {
    // ensure the user isn't trying to update key fields
    // we can update the field, but we can't change the key, it'll just get out of sync
    for (const fieldKey in updateFields) {
      if (this.readOnlyFields.indexOf(fieldKey) !== -1) {
        throw new Error(`Cannot update read-only field ${fieldKey}`);
      }
    }

    const updateFieldsWithMetadata = {
      ...updateFields,
    } as Record<string, UpdateValue>;

    // increment write count in every transaction
    // this allows extensions to perform optimistic locking on entities
    // and re-try if we are interrupting their operation with this write
    updateFieldsWithMetadata["_wc"] = { $inc: 1 };

    // this will throw if the user hasn't provided required keys
    const [pk, sk] = generateIndexStrings(
      this.config.name,
      this.config.partitionKey,
      this.config.sortKey,
      key
    );
    const transaction = this.table.createTransaction({
      type: "Update",
      key: {
        _PK: pk,
        _SK: sk,
      },
      updateValues: updateFieldsWithMetadata,
      // ensure this entity already exists, we're expecting this to be an update
      condition: {
        _PK: { $exists: true },
      },
      errorHandler: (e: any) => {
        if (e instanceof DFConditionalCheckFailedError) {
          throw new Error("Entity does not exist");
        }

        /* istanbul ignore next */
        throw e;
      },
    });

    // run extensions
    this.extensions.map((extension) =>
      extension.onUpdate(key, updateFieldsWithMetadata, transaction)
    );

    // gotta still run postRetrieves on writes
    transaction.resultTransformer = this.entityFromRawDynamoItem.bind(this);

    return transaction;
  }

  public async update(
    key: Partial<Entity>,
    updatedEntity: Partial<Record<keyof Entity, UpdateValue>>
  ): Promise<Entity> {
    const transaction = this.updateTransaction(key, updatedEntity);
    return (await transaction.commit()) as Entity;
  }

  public deleteTransaction(key: Partial<Entity>): DFWriteTransaction {
    // will succeed at deleting an item that doesn't exist by default
    // the transaction can be amended with a check that the item doesn't exist & custom error handler if needed

    const [pk, sk] = generateIndexStrings(
      this.config.name,
      this.config.partitionKey,
      this.config.sortKey,
      key
    );

    const transaction = this.table.createTransaction({
      type: "Delete",
      key: {
        _PK: pk,
        _SK: sk,
      },
    });

    // run extensions
    this.extensions.map((extension) => extension.onDelete(key, transaction));

    return transaction;
  }

  public async delete(key: Partial<Entity>): Promise<void> {
    await this.deleteTransaction(key).commit();
  }

  public async retrieveManyWithPagination(
    query: Query<Entity>
  ): Promise<{ items: Entity[]; lastEvaluatedKey?: Record<string, any> }> {
    if (query.index === undefined && query.rawExpression === undefined) {
      // default expression against the primary index
      query.rawExpression = generateQueryExpression(
        this.config.name,
        this.config.partitionKey,
        this.config.sortKey,
        query
      );
    }

    // run extension hooks
    this.extensions.map((extension) => extension.onQuery(query));

    if (query.rawExpression === undefined) {
      throw new Error(
        `No extensions available to handle querying by index '${query.index}'`
      );
    }
    const queryExpression = query.rawExpression;

    // filters have the same interface as expressions
    const filterExpression = conditionToConditionExpression(
      query.filter as DFCondition
    );

    const result = await this.table.client.query({
      TableName: this.table.tableName,
      KeyConditionExpression: queryExpression.keyConditionExpression,
      FilterExpression: filterExpression.conditionExpression,
      ExpressionAttributeNames: {
        ...queryExpression.expressionAttributeNames,
        ...filterExpression.expressionAttributeNames,
      },
      ExpressionAttributeValues: {
        ...queryExpression.expressionAttributeValues,
        ...filterExpression.expressionAttributeValues,
      },
      Limit: query.limit || undefined,
      ConsistentRead: query.consistentRead,
      IndexName: queryExpression.indexName,
      ExclusiveStartKey: query.exclusiveStartKey,
      ScanIndexForward: query.sort === undefined || query.sort === "ASC",
    });
    const entities = result.Items as EntityWithMetadata[];

    // TODO: test me
    const filteredEntities = this.extensions.reduce(
      (entities, ext) => ext.filterQueryResults(query, entities),
      entities
    );

    // run extensions on all items & strip metadata
    if (query.returnRaw) {
      return {
        items: filteredEntities as Entity[],
        lastEvaluatedKey: result.LastEvaluatedKey,
      };
    }

    const parsedEntities = await Promise.all(
      filteredEntities.map((x) => this.entityFromRawDynamoItem(x))
    );

    return {
      items: parsedEntities,
      lastEvaluatedKey: result.LastEvaluatedKey,
    };
  }

  public async retrieveMany(query: Query<Entity>): Promise<Entity[]> {
    const { items } = await this.retrieveManyWithPagination(query);
    return items;
  }

  public async retrieveOne(query: Query<Entity>): Promise<null | Entity> {
    const items = await this.retrieveMany({
      ...query,
      limit: 1,
    });
    if (items.length === 0) {
      return null;
    }

    return items[0];
  }

  public async retrieveBatch(keys: Array<Partial<Entity>>): Promise<Entity[]> {
    const requestedPrimaryKeys = keys.map((key) => {
      const [pk, sk] = generateIndexStrings(
        this.config.name,
        this.config.partitionKey,
        this.config.sortKey,
        key
      );
      return {
        _PK: pk,
        _SK: sk,
      };
    });

    // should never take more than 2.5 requests to get 100 400kb items
    // we may need to retry further if we get throttled though
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-api
    let retriesRemaining = 5;
    let rawUnsortedItems: EntityWithMetadata[] = [];

    const fetchPksAndAddToUnsortedList = async (
      primaryKeys: Array<{ _PK: string; _SK: string }>
    ) => {
      retriesRemaining -= 1;
      /* istanbul ignore next */
      if (retriesRemaining <= 0) {
        throw new Error("Too many retries");
      }

      const res = await this.table.client.batchGet({
        RequestItems: {
          [this.table.tableName]: {
            Keys: primaryKeys,
          },
        },
      });

      // the spec says this is possible, but I've never seen it happen
      /* istanbul ignore next */
      if (res.Responses === undefined || !res.Responses[this.table.tableName]) {
        throw new Error("No responses returned from batchGet");
      }

      rawUnsortedItems = rawUnsortedItems.concat(
        res.Responses[this.table.tableName]
      );

      // slightly clean up this API by always returning a single array of keys
      // the spec says this is possible, but I've never seen it happen
      /* istanbul ignore next */
      const unprocessedKeys: Array<{ _PK: string; _SK: string }> =
        (res.UnprocessedKeys &&
          res.UnprocessedKeys[this.table.tableName] &&
          (res.UnprocessedKeys[this.table.tableName].Keys as Array<{
            _PK: string;
            _SK: string;
          }>)) ||
        [];

      // TODO: honestly not sure how we can integration test this without loading a table with a lot of data
      /* istanbul ignore next */
      if (unprocessedKeys.length > 0) {
        // process more items!
        await fetchPksAndAddToUnsortedList(unprocessedKeys);
      }
    };

    // will recursively fetch any failed items
    await fetchPksAndAddToUnsortedList(requestedPrimaryKeys);

    const sortedEntities: Entity[] = await Promise.all(
      requestedPrimaryKeys
        .map(
          ({ _PK, _SK }) =>
            rawUnsortedItems.find((x) => x._PK === _PK && x._SK === _SK)!
        )
        .map((entityWithMetadata) =>
          this.entityFromRawDynamoItem(entityWithMetadata)
        )
    );

    return sortedEntities;
  }

  // basically the same as entityFromDynamo row, however it will ALWAYS run migrations
  // entityFromDynamoRow will only run migrations if an extension says it's out of date
  public async migrateEntityWithMetadata(
    entityWithMetadata: DynamoItem
  ): Promise<Entity> {
    const createPrimaryOperation: () => DFWritePrimaryOperation = () => {
      const { _PK, _SK, ...nonKeyProperties } = entityWithMetadata;
      return {
        type: "Update",
        key: {
          _PK,
          _SK,
        },
        updateValues: {
          ...nonKeyProperties,
          _wc: { $inc: 1 },
        },
        // ensure this entity already exists, we're expecting this to be an update
        condition: {
          _wc: { $eq: entityWithMetadata._wc },
        },
        // the _wc check above provides an optimistic lock for our migration
        // if someone updates this entity while we are migrating, we will fail
        // and this error handler will be run
        // the error-handler re-reads from the DB and tries the migration again
        errorHandler: async (e: any) => {
          /* istanbul ignore next */
          if (!(e instanceof DFConditionalCheckFailedError)) {
            throw e; // not our business
          }

          // re-fetch the full entity from the database
          const res = await this.table.client.get({
            TableName: this.table.tableName,
            Key: {
              _PK,
              _SK,
            },
          });

          if (res.Item === undefined) {
            throw new Error(
              "Item was deleted while migration was in progress, migration cancelled"
            );
          }

          // store the latest version of this entity
          entityWithMetadata = res.Item;

          // reset Transaction state
          transaction.primaryOperation = createPrimaryOperation();

          // request a retry
          return RETRY_TRANSACTION;
        },
      };
    };

    // create write transaction
    const transaction = this.table.createTransaction(createPrimaryOperation());

    // ask all our extensions to migrate this item
    // we can commit all migrates in one go
    transaction.addPreCommitHandler(async () => {
      await Promise.all(
        this.extensions.map((extension) =>
          extension.migrateEntity(entityWithMetadata, transaction)
        )
      );
    });

    const migratedEntity = await transaction.commitWithReturn();

    // check our migration was successful
    // if this returns false, it's possible we are stuck in a loop with an extension
    // that will never be happy with this entity
    for (const ext of this.extensions) {
      if (ext.entityRequiresMigration(migratedEntity)) {
        throw new Error(
          `Extension ${ext.constructor.name} still requires migration after migration was run`
        );
      }
    }

    return this.entityFromRawDynamoItem(migratedEntity);
  }

  // runs postRetrieve hooks for all extensions & strip metadata
  public async entityFromRawDynamoItem(
    entityWithMetadata: DynamoItem
  ): Promise<Entity> {
    // check with all extensions to see if any think the entity needs to be migrated
    const entityRequiresMigration = this.extensions.some((extension) =>
      extension.entityRequiresMigration(
        entityWithMetadata as EntityWithMetadata
      )
    );

    if (entityRequiresMigration) {
      // run migrations
      entityWithMetadata = await this.migrateEntityWithMetadata(
        entityWithMetadata
      );
    }

    // run postRetrieve hooks
    await Promise.all(
      this.extensions.map((extension) =>
        extension.postRetrieve(entityWithMetadata)
      )
    );

    // remove any keys that start with '_' (strip metadata)
    const entity: any = {};
    Object.keys(entityWithMetadata)
      .filter((x) => !x.startsWith("_"))
      .forEach(
        (key) => (entity[key] = entityWithMetadata[key as keyof Entity])
      );

    return entity as Entity;
  }
}
