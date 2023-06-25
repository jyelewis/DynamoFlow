import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { generateKeyString } from "../utils/generateKeyString.js";
import {
  EntityWithMetadata,
  Query,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { PartialQueryExpression } from "../types/internalTypes.js";
import { generateQueryExpression } from "../utils/generateQueryExpression.js";
import { DFCollection } from "../DFCollection.js";

interface DFSecondaryIndexExtConfig<Entity extends SafeEntity<Entity>> {
  indexName: string;
  dynamoIndex: "GSI1" | "GSI2" | "GSI3" | "GSI4" | "GSI5";
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
  includeInIndex?: (entity: Entity) => boolean;
}

export class DFSecondaryIndexExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  public constructor(
    private readonly config: DFSecondaryIndexExtConfig<Entity>
  ) {
    super();
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
      !this.config.includeInIndex(entity as Entity)
    ) {
      // don't write to this secondary index
      // leave out keys, Dynamo will not write to the index if the keys are missing
      return;
    }

    entity[this.indexPartitionKey] =
      `${this.collection.config.name}#` +
      generateKeyString(this.config.partitionKey, entity);

    entity[this.indexSortKey] =
      `${this.collection.config.name}#` +
      generateKeyString(this.config.sortKey || [], entity);
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ): void | Promise<void> {
    // TODO: we have the whole key, merge with entityUpdate to see if any properties will change
    // then compute the new key & persist if needed
    // also, run should index check here - we might even need to remove it from the index
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

  private get indexPartitionKey(): string {
    return `_${this.config.dynamoIndex}PK`;
  }

  private get indexSortKey(): string {
    return `_${this.config.dynamoIndex}SK`;
  }
}
