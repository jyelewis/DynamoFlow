import {
  testCreatedInTestDbConfig,
  testCreatedInTestGSIsDbConfig,
  testDbConfig,
  testFullTableScanDbConfig,
} from "../testHelpers/testDbConfigs.js";
import { DFTable } from "../DFTable.js";
import { FullTableScanItem, STOP_SCAN } from "../types/types.js";
import { setTimeout } from "timers/promises";
import {
  DeleteTableCommand,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";

describe("DFTable", () => {
  describe("Basics", () => {
    it("Constructs", () => {
      const table = new DFTable(testDbConfig);
      expect(table.config).toEqual(testDbConfig);
      expect(table.client).not.toBeUndefined();
    });

    it("Creates transaction", () => {
      const table = new DFTable(testDbConfig);
      const transaction = table.createTransaction({
        type: "Update",
        key: { id: 1 },
        updateValues: { name: "Joe" },
      });

      expect(transaction.table).toStrictEqual(table);
      expect(transaction.primaryOperation).toEqual({
        type: "Update",
        key: { id: 1 },
        updateValues: { name: "Joe" },
      });
    });

    it("Creates a collection", () => {
      const table = new DFTable(testDbConfig);

      interface MyEntity {
        id: number;
      }

      const collection = table.createCollection<MyEntity>({
        name: "MyCollection",
        partitionKey: "id",
        extensions: [],
      });

      expect(collection.table).toStrictEqual(table);
      expect(Object.keys(table.collections)).toEqual(["MyCollection"]);
      expect(table.collections["MyCollection"]).toStrictEqual(collection);
      expect(collection.config.name).toEqual("MyCollection");
    });
  });

  describe("fullTableScan", () => {
    const table = new DFTable(testFullTableScanDbConfig);

    interface User {
      id: number;
      fullName?: string; // this will start empty and be populated by a migration
      firstName: string;
      lastName: string;
    }

    interface Project {
      id: number;
      name: string;
    }

    const usersCollection = table.createCollection<User>({
      name: "users",
      partitionKey: "id",
      extensions: [],
    });
    const projectsCollection = table.createCollection<Project>({
      name: "projects",
      partitionKey: "id",
      extensions: [],
    });

    // annoying trick to get 'beforeAll' behaviour from jest.concurrent
    const allItemsProm: Promise<Array<User | Project>> = (async () => {
      let lastId = 0;
      const addUser = (firstName: string, lastName: string) => {
        return usersCollection.insert(
          {
            id: ++lastId,
            firstName,
            lastName,
            fullName: "",
          },
          {
            allowOverwrite: true,
          }
        );
      };

      const addProject = (name: string) => {
        return projectsCollection.insert(
          {
            id: ++lastId,
            name,
          },
          {
            allowOverwrite: true,
          }
        );
      };

      // insert raw item without a collection
      await table.client.put({
        TableName: table.tableName,
        Item: {
          _PK: "no-collection",
          _SK: "no-collection",
        },
      });

      return await Promise.all([
        addUser("Walter", "White"),
        addUser("Jesse", "Pinkman"),
        addUser("Saul", "Goodman"),
        addUser("Gus", "Fring"),
        addUser("Mike", "Ehrmantraut"),
        addUser("Hank", "Schrader"),
        addUser("Skyler", "White"),
        addUser("Marie", "Schrader"),
        addUser("Walter Jr.", "White"),
        addUser("Hector", "Salamanca"),
        addProject("Project 1"),
        addProject("Project 2"),
        addProject("Project 3"),
        addProject("Spartan"),
        addProject("Build tower"),
      ]);
    })();

    it.concurrent.each([1, 2, 3, 5, 10, 14, 20])(
      `Scans with batch size %i`,
      async (maxBatchSize: number) => {
        const allItems = await allItemsProm;

        let itemsReceived: Array<any> = [];

        await table.fullTableScan({
          processBatch: async (items: FullTableScanItem[]) => {
            expect(items.length).toBeLessThanOrEqual(maxBatchSize);

            itemsReceived = itemsReceived.concat(items);
          },
          maxBatchSize,
        });

        // check we got all items once
        expect(itemsReceived.length).toEqual(allItems.length + 1);
        expect(
          allItems.every((item) =>
            itemsReceived.some((x) => x.entity.id === item.id)
          )
        ).toEqual(true);
      }
    );

    it.concurrent(
      "Scans with large batch & slow processing function",
      async () => {
        const allItems = await allItemsProm;

        let numBatchesReceived = 0;
        let itemsReceived: Array<any> = [];

        await table.fullTableScan({
          processBatch: async (items: FullTableScanItem[]) => {
            itemsReceived = itemsReceived.concat(items);
            numBatchesReceived += 1;

            // make the process function take longer that DDB takes
            // this should slow the scan down, while still fetching all items
            await setTimeout(50);
          },
        });

        expect(numBatchesReceived).toEqual(1);

        // check we got all items once
        expect(itemsReceived.length).toEqual(allItems.length + 1);
        expect(
          allItems.every((item) =>
            itemsReceived.some((x) => x.entity.id === item.id)
          )
        ).toEqual(true);
      }
    );

    it.concurrent(
      "Scans with small batch & slow processing function",
      async () => {
        const allItems = await allItemsProm;

        let numBatchesReceived = 0;
        let itemsReceived: Array<any> = [];

        await table.fullTableScan({
          processBatch: async (items: FullTableScanItem[]) => {
            itemsReceived = itemsReceived.concat(items);
            numBatchesReceived += 1;

            // make the process function take longer that DDB takes
            // this should slow the scan down, while still fetching all items
            await setTimeout(50);
          },
          maxBatchSize: 3,
        });

        expect(numBatchesReceived).toEqual(6);

        // check we got all items once
        expect(itemsReceived.length).toEqual(allItems.length + 1);
        expect(
          allItems.every((item) =>
            itemsReceived.some((x) => x.entity.id === item.id)
          )
        ).toEqual(true);
      }
    );

    it.concurrent("Applies filterExpression _c = 'users'", async () => {
      const allItems = await allItemsProm;

      let numBatchesReceived = 0;
      let itemsReceived: Array<any> = [];

      await table.fullTableScan({
        processBatch: async (items: FullTableScanItem[]) => {
          itemsReceived = itemsReceived.concat(items);
          numBatchesReceived += 1;
        },
        filter: {
          _c: "users",
        },
      });

      expect(numBatchesReceived).toEqual(1);

      // check we got all items once
      expect(itemsReceived.length).toEqual(
        allItems.filter((x) => "firstName" in x).length
      );
      expect(
        itemsReceived.every((x) =>
          allItems.some((item) => x.entity.id === item.id)
        )
      ).toEqual(true);
    });

    it.concurrent("Returns all items with a sparse filter", async () => {
      await allItemsProm;

      let numBatchesReceived = 0;
      let itemsReceived: Array<any> = [];

      await table.fullTableScan({
        processBatch: async (items: FullTableScanItem[]) => {
          itemsReceived = itemsReceived.concat(items);
          numBatchesReceived += 1;
        },
        filter: {
          firstName: "Gus",
        },
      });

      // empty batches should be skipped
      expect(numBatchesReceived).toEqual(1);

      // check we got all items once
      expect(itemsReceived.length).toEqual(1);
      expect(itemsReceived[0].collection).toStrictEqual(usersCollection);
      expect(itemsReceived[0].entity.lastName).toEqual("Fring");
    });

    it.concurrent(
      "Returns all items with a sparse filter & small batch size",
      async () => {
        await allItemsProm;

        let numBatchesReceived = 0;
        let itemsReceived: Array<any> = [];

        await table.fullTableScan({
          processBatch: async (items: FullTableScanItem[]) => {
            itemsReceived = itemsReceived.concat(items);
            numBatchesReceived += 1;
          },
          filter: {
            firstName: "Gus",
          },
          maxBatchSize: 2,
        });

        // we should have only seen 1 batch
        // as the process function isn't called for empty batches
        // empty batches should still be accepted though,
        // as we need to search for the responses with our items
        expect(numBatchesReceived).toEqual(1);

        // check we got all items once
        expect(itemsReceived.length).toEqual(1);
        expect(itemsReceived[0].collection).toStrictEqual(usersCollection);
        expect(itemsReceived[0].entity.lastName).toEqual("Fring");
      }
    );

    it.concurrent("Exists early if STOP_SCAN is returned", async () => {
      await allItemsProm;

      let numBatchesReceived = 0;
      let itemsReceived: Array<any> = [];

      await table.fullTableScan({
        processBatch: async (items: FullTableScanItem[]) => {
          itemsReceived = itemsReceived.concat(items);
          numBatchesReceived += 1;

          if (numBatchesReceived == 2) {
            return STOP_SCAN;
          }
        },
        maxBatchSize: 3,
      });

      expect(numBatchesReceived).toEqual(2); // 2 from early stop
    });

    it.concurrent("Handles items without a collection property", async () => {
      const allItems = await allItemsProm;

      let numItemsWithCollectionReturned = 0;
      let numItemsWithoutCollectionReturned = 0;

      await table.fullTableScan({
        processBatch: async (items: FullTableScanItem[]) => {
          for (const item of items) {
            if (item.collection) {
              numItemsWithCollectionReturned += 1;
            } else {
              numItemsWithoutCollectionReturned += 1;
            }
          }
        },
      });

      expect(numItemsWithCollectionReturned).toEqual(allItems.length);
      expect(numItemsWithoutCollectionReturned).toEqual(1);
    });

    it.concurrent("Can migrate data during scan", async () => {
      // run our migration
      await table.fullTableScan({
        processBatch: async (items: FullTableScanItem[]) => {
          await Promise.all(
            items.map(async ({ collection, entity }) => {
              if (collection === usersCollection) {
                await collection.update(
                  {
                    id: entity.id,
                  },
                  {
                    fullName: `${entity.firstName} ${entity.lastName}`,
                  }
                );
              }
            })
          );
        },
      });

      // check and item to confirm it was updated
      const user1 = await usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      });
      expect(user1?.fullName).toEqual("Walter White");
    });

    it.concurrent("Can run a full table migration", async () => {
      await allItemsProm;

      // we pretty much just expect this to not crash as it is a common pattern
      // this also checks our optimistic locking works - we are performing another scan & update in the concurrent test above
      await table.fullTableScan({
        returnRaw: true, // so we can trigger manual migrations
        processBatch: async (items: FullTableScanItem[]) => {
          await Promise.all(
            items.map(({ entity, collection }) =>
              collection?.migrateEntityWithMetadata(entity)
            )
          );
        },
      });
    });
  });

  describe("createTableIfNotExists", () => {
    it.concurrent("Works with existing table", async () => {
      const existingTable = new DFTable(testDbConfig);

      // should exist both before and after this function is called
      const tablesBeforeCreate = await existingTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesBeforeCreate.TableNames).toContain(existingTable.tableName);

      await existingTable.createTableIfNotExists();

      const tablesAfterCreate = await existingTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesAfterCreate.TableNames).toContain(existingTable.tableName);
    });

    it.concurrent("Works with non-existent table (no GSIs)", async () => {
      const createdInTestTable = new DFTable(testCreatedInTestDbConfig);

      // should exist both before and after this function is called
      const tablesBeforeCreate = await createdInTestTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesBeforeCreate.TableNames).not.toContain(
        createdInTestTable.tableName
      );

      await createdInTestTable.createTableIfNotExists();

      const tablesAfterCreate = await createdInTestTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesAfterCreate.TableNames).toContain(
        createdInTestTable.tableName
      );

      // delete the table so tests can run again
      await createdInTestTable.client.send(
        new DeleteTableCommand({
          TableName: createdInTestTable.tableName,
        })
      );
    });

    it.concurrent("Works with non-existent table (with GSIs)", async () => {
      const createdInTestTable = new DFTable(testCreatedInTestGSIsDbConfig);

      // should exist both before and after this function is called
      const tablesBeforeCreate = await createdInTestTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesBeforeCreate.TableNames).not.toContain(
        createdInTestTable.tableName
      );

      await createdInTestTable.createTableIfNotExists();

      const tablesAfterCreate = await createdInTestTable.client.send(
        new ListTablesCommand({})
      );
      expect(tablesAfterCreate.TableNames).toContain(
        createdInTestTable.tableName
      );

      // delete the table so tests can run again
      await createdInTestTable.client.send(
        new DeleteTableCommand({
          TableName: createdInTestTable.tableName,
        })
      );
    });
  });
});
