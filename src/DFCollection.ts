import { DFBaseExtension } from "./extensions/DFBaseExtension.js";
import { DFDB } from "./DFDB.js";
import { DFWriteTransaction } from "./DFWriteTransaction.js";
import {
  DynamoItem,
  EntityWithMetadata,
  Query,
  SafeEntity,
  UpdateValue,
} from "./types/types.js";
import { PartialQueryExpression } from "./types/internalTypes.js";
import { generateIndexStrings } from "./utils/generateIndexStrings.js";
import { generateQueryExpression } from "./utils/generateQueryExpression.js";
import { DFUpdateOperation } from "./types/operations.js";

export interface DFCollectionConfig<Entity extends SafeEntity<Entity>> {
  name: string;
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
  extensions: DFBaseExtension<Entity>[];
}

export class DFCollection<Entity extends SafeEntity<Entity>> {
  constructor(
    public readonly db: DFDB,
    public readonly config: DFCollectionConfig<Entity>
  ) {
    // init extensions
    config.extensions.forEach((extension) => extension.init(this));

    // TODO: validate this DB doesn't have any collections wth a conflicting name
  }

  // kind of annoying this can't be sync, extensions may need to do pre-fetches
  public async insertTransaction(
    newEntity: Entity,
    options?: {
      allowOverwrite?: boolean;
    }
  ): Promise<DFWriteTransaction> {
    const entityWithMetadata: EntityWithMetadata<Entity> = { ...newEntity };

    // TODO: test this
    // TODO: why isn't TS happy with this?
    // used for table scans so we can call the appropriate collection to handle this entity
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    entityWithMetadata["_collection"] = this.config.name;
    // allows extensions to perform optimistic locking on entities
    // without storing extra metadata properties themselves
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    entityWithMetadata["_writeCount"] = 1;

    const allowOverwrite = !options || !options.allowOverwrite;

    const [pk, sk] = generateIndexStrings(
      this.config.name,
      this.config.partitionKey,
      this.config.sortKey,
      entityWithMetadata
    );
    const transaction = this.db.createTransaction({
      type: "Update",
      key: {
        _PK: pk,
        _SK: sk,
      },
      entity: entityWithMetadata,
      // ensure this entity doesn't already exist! We're creating it
      // can we get away without using conditionExpressionAttributeNames??
      conditionExpression: allowOverwrite
        ? "attribute_not_exists(#PK)"
        : undefined,
      conditionExpressionAttributeNames: allowOverwrite
        ? {
            "#PK": "_PK",
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
      Object.values(this.config.extensions).map((extension) =>
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

  public async updateTransaction(
    key: Partial<Entity>,
    updatedEntity: Partial<Record<keyof Entity, UpdateValue>>
  ): Promise<DFWriteTransaction> {
    const partialEntityWithMetadata: EntityWithMetadata<typeof updatedEntity> =
      {
        ...updatedEntity,
      };

    // TODO: test this
    // TODO: why isn't TS happy with this?
    // increment write count in every transaction
    // this allows extensions to perform optimistic locking on entities
    // and re-try if we are interrupting their operation with this write
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    partialEntityWithMetadata["_writeCount"] = { $inc: 1 };

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
      entity: partialEntityWithMetadata,
      // ensure this entity already exists, we're expecting this to be an update
      conditionExpression: "attribute_exists(#PK)",
      conditionExpressionAttributeNames: {
        "#PK": "_PK",
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
      Object.values(this.config.extensions).map((extension) =>
        extension.onUpdate(key, partialEntityWithMetadata, transaction)
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
    const transaction = await this.updateTransaction(keys, updatedEntity);
    return (await transaction.commit()) as Entity;
  }

  // TODO: delete operations

  // TODO: pagination
  public async retrieveMany(query: Query<Entity>): Promise<Entity[]> {
    // generate an expression based off the query
    // if this query is against the primary index, that will generate it locally
    // otherwise it will search for an extension to generate this expression for us
    const queryExpression = await this.expressionForQuery(query);

    const result = await this.db.client.query({
      TableName: this.db.tableName,
      KeyConditionExpression: queryExpression.keyConditionExpression,
      ExpressionAttributeNames: queryExpression.expressionAttributeNames,
      ExpressionAttributeValues: queryExpression.expressionAttributeValues,
      Limit: query.limit || undefined,
      ConsistentRead: query.consistentRead,
      IndexName: queryExpression.indexName,
    });
    const entities = result.Items as EntityWithMetadata<Entity>[];

    // run extensions on all items & strip metadata
    return await Promise.all(
      entities.map((x) => this.entityFromRawDynamoItem(x))
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

  // runs postRetrieve hooks for all extensions & strip metadata
  public async entityFromRawDynamoItem(
    entityWithMetadata: DynamoItem
  ): Promise<Entity> {
    // TODO: this is often run in a loop, if many items need updating it would be more efficient to batch write them

    // run postRetrieve hooks
    await Promise.all(
      Object.values(this.config.extensions).map((extension) =>
        extension.postRetrieve(entityWithMetadata as EntityWithMetadata<Entity>)
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