import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import {
  CreateTableCommand,
  CreateTableCommandInput,
  DynamoDBClient,
  ListTablesCommand,
  ScanCommandInput,
  UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import { DFWriteTransaction } from "./DFWriteTransaction.js";
import { DFCollection, DFCollectionConfig } from "./DFCollection.js";
import {
  FullTableScan,
  FullTableScanItem,
  SafeEntity,
  STOP_SCAN,
} from "./types/types.js";
import { DFWritePrimaryOperation } from "./types/operations.js";
import { conditionToConditionExpression } from "./utils/conditionToConditionExpression.js";

export interface DFTableConfig {
  tableName: string;
  region?: string;
  endpoint?: string;
  GSIs?: string[];
  // TODO: test this
  keyPrefix?: string;
}

export class DFTable {
  public readonly client: DynamoDBDocument;
  public readonly collections: Record<string, DFCollection<any>> = {};

  constructor(public readonly config: DFTableConfig) {
    const rawDbClient = new DynamoDBClient({
      region: config.region,
      endpoint: config.endpoint,
    });

    // AWSs improved wrapper over the raw dynamo client
    // auto unwraps types, provides convenience methods for all core operations
    this.client = DynamoDBDocument.from(rawDbClient, {
      marshallOptions: {
        removeUndefinedValues: true,
      },
    });
  }

  public get tableName() {
    return this.config.tableName;
  }

  public createTransaction(
    primaryOperation: DFWritePrimaryOperation
  ): DFWriteTransaction {
    return new DFWriteTransaction(this, primaryOperation);
  }

  public createCollection<Entity extends SafeEntity<Entity>>(
    collectionConfig: DFCollectionConfig<Entity>
  ): DFCollection<Entity> {
    // TODO: test me
    if (this.config.keyPrefix) {
      collectionConfig.name = `${this.config.keyPrefix}${collectionConfig.name}`;
    }
    const newCollection = new DFCollection(this, collectionConfig);

    // keep a reference ourselves
    this.collections[collectionConfig.name] = newCollection;

    return newCollection;
  }

  public async fullTableScan(scanParams: FullTableScan): Promise<void> {
    let lastEvaluatedKey: undefined | ScanCommandInput["ExclusiveStartKey"];

    // try to keep a new batch ready
    let moreDataLeftToFetch = true;
    let isComplete = false;
    let nextBatch: FullTableScanItem[] = [];

    const fetchNextBatch = async () => {
      if (!moreDataLeftToFetch) {
        nextBatch = [];
        return;
      }

      const {
        conditionExpression,
        expressionAttributeNames,
        expressionAttributeValues,
      } = conditionToConditionExpression(scanParams.filter);

      const scanRes = await this.client.scan({
        TableName: this.tableName,
        ExclusiveStartKey: lastEvaluatedKey,
        Limit: scanParams.maxBatchSize,
        FilterExpression: conditionExpression,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
        Segment: scanParams.segment,
        TotalSegments: scanParams.totalSegments,
        ReturnConsumedCapacity: "TOTAL",
        IndexName: scanParams.dynamoIndex,
      });
      lastEvaluatedKey = scanRes.LastEvaluatedKey;
      if (lastEvaluatedKey === undefined) {
        // no pages left
        moreDataLeftToFetch = false;
      }

      // the spec says this is possible, but I've never seen it happen
      /* istanbul ignore next */
      if (!scanRes.Items) {
        // continue scanning, all items in this batch may have been filtered
        nextBatch = [];
        return;
      }

      // process all items from this scan response
      // store as the next batch once we are ready
      nextBatch = await Promise.all(
        scanRes.Items.map(async (item) => {
          const collectionName = item._c as string;
          const collectionForItem = this.collections[collectionName];
          if (collectionForItem === undefined) {
            return {
              // return the raw item if we don't know whats going on with it
              collection: undefined,
              entity: item,
            };
          }

          if (scanParams.returnRaw) {
            return {
              collection: collectionForItem,
              entity: item as SafeEntity<any>,
            };
          }

          const entity = (await collectionForItem.entityFromRawDynamoItem(
            item
          )) as SafeEntity<any>;

          return {
            collection: collectionForItem,
            entity,
          };
        })
      );
    };

    // fetch first batch
    await fetchNextBatch();

    // continue processing batches until we are complete
    while (!isComplete) {
      const [processBatchResponse] = await Promise.all([
        // process the batch we have available
        nextBatch.length > 0 ? scanParams.processBatch(nextBatch) : undefined,

        // fetch the next batch in advance while we wait
        // no-op if there is no data left to fetch
        fetchNextBatch(),
      ]);

      if (!moreDataLeftToFetch && nextBatch.length === 0) {
        // we have no more data to fetch and no more data to process
        isComplete = true;
      }

      if (processBatchResponse === STOP_SCAN) {
        break;
      }
    }
  }

  public async createTableIfNotExists() {
    const tables = await this.client.send(new ListTablesCommand({}));

    if (tables.TableNames && tables.TableNames.indexOf(this.tableName) !== -1) {
      // table already exists, nothing to do :)
      return;
    }

    const attributeDefinitions: CreateTableCommandInput["AttributeDefinitions"] =
      [
        { AttributeName: "_PK", AttributeType: "S" },
        { AttributeName: "_SK", AttributeType: "S" },
      ];

    if (this.config.GSIs) {
      this.config.GSIs.forEach((gsi) => {
        attributeDefinitions.push(
          { AttributeName: `_${gsi}PK`, AttributeType: "S" },
          { AttributeName: `_${gsi}SK`, AttributeType: "S" }
        );
      });
    }

    try {
      await this.client.send(
        new CreateTableCommand({
          TableName: this.tableName,
          BillingMode: "PAY_PER_REQUEST",
          KeySchema: [
            { AttributeName: "_PK", KeyType: "HASH" },
            { AttributeName: "_SK", KeyType: "RANGE" },
          ],
          AttributeDefinitions: attributeDefinitions,
          GlobalSecondaryIndexes: this.config.GSIs?.map((gsi) => ({
            IndexName: gsi,
            KeySchema: [
              { AttributeName: `_${gsi}PK`, KeyType: "HASH" },
              { AttributeName: `_${gsi}SK`, KeyType: "RANGE" },
            ],
            Projection: {
              ProjectionType: "ALL",
            },
          })),
        })
      );

      // enable ttl
      await this.client.send(
        new UpdateTimeToLiveCommand({
          TableName: this.tableName,
          TimeToLiveSpecification: {
            AttributeName: "_ttl",
            Enabled: true,
          },
        })
      );
    } catch (e) {
      // swallow if table already exists
      // other test threads often race us to the check
    }
  }
}
