import { DFBaseExtension } from "./extensions/DFBaseExtension.js";
import { DFDB } from "./DFDB.js";
import { DFWriteTransaction } from "./DFWriteTransaction.js";
import {
  DynamoItem,
  EntityWithMetadata,
  Query,
  RETRY_TRANSACTION,
  SafeEntity,
  UpdateValue,
} from "./types/types.js";
import { PartialQueryExpression } from "./types/internalTypes.js";
import { generateIndexStrings } from "./utils/generateIndexStrings.js";
import { generateQueryExpression } from "./utils/generateQueryExpression.js";
import {
  DFCondition,
  DFUpdateOperation,
  DFWritePrimaryOperation,
} from "./types/operations.js";
import { conditionToConditionExpression } from "./utils/conditionToConditionExpression.js";

export interface DFCollectionConfig<Entity extends SafeEntity<Entity>> {
  name: string;
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
  // TODO: can this be optional?
  extensions: DFBaseExtension<Entity>[];
}

export class DFCollection<Entity extends SafeEntity<Entity>> {
  constructor(
    public readonly db: DFDB,
    public readonly config: DFCollectionConfig<Entity>
  ) {
    if (this.config.name in this.db.collections) {
      // TODO: test me
      throw new Error(
        `Collection '${this.config.name}' already exists in this DB`
      );
    }

    // init extensions
    config.extensions.forEach((extension) => extension.init(this));
  }

  // TODO: do these need to be async anymore?
  public async insertTransaction(
    newEntity: Entity,
    options?: {
      allowOverwrite?: boolean;
    }
  ): Promise<DFWriteTransaction> {
    const entityWithMetadata: EntityWithMetadata = { ...newEntity };

    // TODO: test this
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
    const transaction = this.db.createTransaction({
      // TODO: TS doesn't seem to warn about invalid properties here...
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
        if (e.Code === "ConditionalCheckFailedException") {
          throw new Error("Entity already exists");
        }

        /* istanbul ignore next */
        throw e;
      },
    } as DFUpdateOperation);

    // run extensions
    await Promise.all(
      this.config.extensions.map((extension) =>
        extension.onInsert(entityWithMetadata, transaction)
      )
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
    const transaction = await this.insertTransaction(newEntity, options);
    return (await transaction.commit()) as Entity;
  }

  // TODO: these don't need to be async anymore!
  public async updateTransaction(
    key: Partial<Entity>,
    updateFields: Partial<Record<keyof Entity, UpdateValue>>
  ): Promise<DFWriteTransaction> {
    const updateFieldsWithMetadata = {
      ...updateFields,
    } as Record<string, UpdateValue>;

    // TODO: test this
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
    const transaction = this.db.createTransaction({
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
        if (e.Code === "ConditionalCheckFailedException") {
          throw new Error("Entity does not exist");
        }

        /* istanbul ignore next */
        throw e;
      },
    });

    // run extensions
    await Promise.all(
      this.config.extensions.map((extension) =>
        extension.onUpdate(key, updateFieldsWithMetadata, transaction)
      )
    );

    // gotta still run postRetrieves on writes
    transaction.resultTransformer = this.entityFromRawDynamoItem.bind(this);

    return transaction;
  }

  public async update(
    keys: Partial<Entity>,
    updatedEntity: Partial<Record<keyof Entity, UpdateValue>>
  ): Promise<Entity> {
    // TODO: is it dangerous to perform migrations after an update? What if we write to a new field then a migration takes the old field value
    const transaction = await this.updateTransaction(keys, updatedEntity);
    return (await transaction.commit()) as Entity;
  }

  // TODO: delete operations

  // TODO: pagination
  // TODO: sort direction
  public async retrieveMany(query: Query<Entity>): Promise<Entity[]> {
    // generate an expression based off the query
    // if this query is against the primary index, that will generate it locally
    // otherwise it will search for an extension to generate this expression for us
    const queryExpression = await this.expressionForQuery(query);

    // filters have the same interface as expressions
    const filterExpression = conditionToConditionExpression(
      query.filter as DFCondition
    );

    const result = await this.db.client.query({
      TableName: this.db.tableName,
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
    });
    const entities = result.Items as EntityWithMetadata[];

    // run extensions on all items & strip metadata
    if (query.returnRaw) {
      return entities as Entity[];
    }

    return await Promise.all(
      entities.map((x) =>
        // TODO: test me
        this.entityFromRawDynamoItem(x)
      )
    );
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

  // TODO: test me
  public async migrateEntity(entityWithMetadata: DynamoItem): Promise<Entity> {
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
          _PK: { $exists: true },
          _wc: { $eq: entityWithMetadata._wc },
        },
        // the _wc check above provides an optimistic lock for our migration
        // if someone updates this entity while we are migrating, we will fail
        // and this error handler will be run
        // the error-handler re-reads from the DB and tries the migration again
        errorHandler: async (e: any) => {
          if (e.Code === "ConditionalCheckFailedException") {
            // re-fetch the full entity from the database
            const res = await this.db.client.get({
              TableName: this.db.tableName,
              Key: {
                _PK: nonKeyProperties,
                _SK: nonKeyProperties,
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
          }

          /* istanbul ignore next */
          throw e;
        },
      };
    };

    // create write transaction
    const transaction = this.db.createTransaction(createPrimaryOperation());

    // ask all our extensions to migrate this item
    // we can commit all migrates in one go
    transaction.addPreCommitHandler(async () => {
      await Promise.all(
        this.config.extensions.map((extension) =>
          extension.migrateEntity(entityWithMetadata, transaction)
        )
      );
    });

    const migratedEntity = await transaction.commitWithReturn();

    // check our migration was successful
    // if this returns false, it's possible we are stuck in a loop with an extension
    // that will never be happy with this entity
    for (const ext of this.config.extensions) {
      if (ext.entityRequiresMigration(migratedEntity)) {
        throw new Error(
          `Extension ${ext.constructor.name} still requires migration after migration was run`
        );
      }
    }

    return this.entityFromRawDynamoItem(migratedEntity);
  }

  // TODO: test me with migrations
  // runs postRetrieve hooks for all extensions & strip metadata
  public async entityFromRawDynamoItem(
    entityWithMetadata: DynamoItem
  ): Promise<Entity> {
    // TODO: this is often run in a loop, if many items need updating it would be more efficient to batch write them

    // check with all extensions to see if any think the entity needs to be migrated
    const entityRequiresMigration = this.config.extensions.some((extension) =>
      extension.entityRequiresMigration(
        entityWithMetadata as EntityWithMetadata
      )
    );

    if (entityRequiresMigration) {
      // run migrations
      entityWithMetadata = await this.migrateEntity(entityWithMetadata);
    }

    // run postRetrieve hooks
    await Promise.all(
      this.config.extensions.map((extension) =>
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

  private async expressionForQuery(
    query: Query<Entity>
  ): Promise<PartialQueryExpression> {
    if (query.index === undefined) {
      // primary index
      return generateQueryExpression(
        this.config.name,
        this.config.partitionKey,
        this.config.sortKey,
        query
      );
    }

    for (const extension of this.config.extensions) {
      // ask this extension if they can provide an expression for this query
      const queryExpression = await extension.expressionForQuery(query);
      if (queryExpression !== undefined) {
        return queryExpression;
      }
    }

    throw new Error(
      `No extensions available to handle querying by index '${query.index}'`
    );
  }
}
