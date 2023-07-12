import { DFTable } from "../DFTable.js";
import { DFSecondaryIndexExt } from "../extensions/DFSecondaryIndexExt.js";
import { DFMigrationExt } from "../extensions/DFMigrationExt.js";
import { DFBaseExtension } from "../extensions/DFBaseExtension.js";
import { testDbConfigWithPrefix } from "../testHelpers/testDbConfigs.js";

interface User {
  id: number;
  firstName: string;
  lastName: string;
  isActivated: boolean;
  age?: number;
  lastUpdated?: string;
}
interface Thing {
  groupId: number;
  thingId: number;
}

describe("DFCollection", () => {
  describe("Construct", () => {
    it("Throws if duplicate collection is created within the same table", () => {
      const table = new DFTable(testDbConfigWithPrefix());

      expect(() =>
        table.createCollection<User>({
          name: "user",
          partitionKey: "id",
        })
      ).not.toThrow();

      expect(() =>
        table.createCollection<User>({
          name: "user",
          partitionKey: "id",
        })
      ).toThrow("already exists in this table");
    });
  });

  describe("Insert", () => {
    it.concurrent("Can insert items", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
      });

      const insertedUser = await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });
      expect(insertedUser).toEqual({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });
    });

    it.concurrent(
      "Throws if inserting an item that already exists",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const usersCollection = table.createCollection<User>({
          name: `user`,
          partitionKey: "id",
        });

        await usersCollection.insert({
          id: 1,
          firstName: "Jye",
          lastName: "Lewis",
          isActivated: true,
        });

        // try to insert again
        await expect(
          usersCollection.insert({
            id: 1,
            firstName: "Jye",
            lastName: "Lewis",
            isActivated: true,
          })
        ).rejects.toThrow("Entity already exists");
      }
    );

    it.concurrent(
      "Can insert multiple items within a transaction",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const usersCollection = table.createCollection<User>({
          name: `user`,
          partitionKey: "id",
        });

        const transaction = usersCollection.insertTransaction({
          id: 1,
          firstName: "Jye",
          lastName: "Lewis",
          isActivated: true,
        });
        transaction.addSecondaryTransaction(
          usersCollection.insertTransaction({
            id: 2,
            firstName: "Joe",
            lastName: "Bot",
            isActivated: false,
          })
        );

        const insertedUser = await transaction.commitWithReturn();
        expect(insertedUser).toEqual({
          id: 1,
          firstName: "Jye",
          lastName: "Lewis",
          isActivated: true,
        });

        // fetch back the second item and validate
        const secondInsertedItem = await usersCollection.retrieveOne({
          where: {
            id: 2,
          },
          consistentRead: true,
        });
        expect(secondInsertedItem).toEqual({
          id: 2,
          firstName: "Joe",
          lastName: "Bot",
          isActivated: false,
        });
      }
    );

    it.concurrent("Stores _wc and _c metadata properties", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: "user",
        partitionKey: "id",
      });
      const testCollectionName = usersCollection.config.name;

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });

      const rawUser = await usersCollection.retrieveOne({
        where: {
          id: 1,
        },
        returnRaw: true,
      });
      expect(rawUser).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _wc: 1,
        _c: testCollectionName,
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });
    });
  });

  describe("Update", () => {
    it.concurrent("Can update items", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
      });

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        age: 30,
      });

      const updatedUser = await usersCollection.update(
        { id: 1 },
        {
          firstName: "Jye 2",
          age: { $inc: 10 },
        }
      );
      expect(updatedUser).toEqual({
        id: 1,
        firstName: "Jye 2",
        lastName: "Lewis",
        age: 40,
        isActivated: true,
      });
    });

    it.concurrent(
      "Throws if updating an item that does not exist",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const usersCollection = table.createCollection<User>({
          name: `user`,
          partitionKey: "id",
        });

        await expect(
          usersCollection.update(
            { id: 1 },
            {
              firstName: "Jye 2",
              age: { $inc: 10 },
            }
          )
        ).rejects.toThrow("Entity does not exist");
      }
    );

    it.concurrent("Throws if updating a field used within key", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
      });

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        age: 30,
      });

      await expect(
        usersCollection.update(
          { id: 1 },
          {
            id: 2,
            firstName: "Jye 2",
            age: { $inc: 10 },
          }
        )
      ).rejects.toThrow("Cannot update read-only field id");
    });

    it.concurrent(
      "Can update multiple items within a transaction",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const usersCollection = table.createCollection<User>({
          name: `user`,
          partitionKey: "id",
        });

        await usersCollection.insert({
          id: 1,
          firstName: "Jye",
          lastName: "Lewis",
          isActivated: true,
          age: 30,
        });
        await usersCollection.insert({
          id: 2,
          firstName: "Joe",
          lastName: "Bot",
          isActivated: false,
        });

        const transaction = usersCollection.updateTransaction(
          { id: 1 },
          {
            firstName: "Jye 2",
            age: { $inc: 10 },
          }
        );
        transaction.addSecondaryTransaction(
          usersCollection.updateTransaction(
            {
              id: 2,
            },
            {
              isActivated: true,
            }
          )
        );

        const updatedUser = await transaction.commit();
        expect(updatedUser).toEqual({
          id: 1,
          firstName: "Jye 2",
          lastName: "Lewis",
          age: 40,
          isActivated: true,
        });

        // fetch back the second item and validate
        const secondInsertedItem = await usersCollection.retrieveOne({
          where: {
            id: 2,
          },
          consistentRead: true,
        });
        expect(secondInsertedItem).toEqual({
          id: 2,
          firstName: "Joe",
          lastName: "Bot",
          isActivated: true,
        });
      }
    );

    it.concurrent("Updates _wc", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: "user",
        partitionKey: "id",
      });
      const testCollectionName = usersCollection.config.name;

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        age: 30,
      });

      await usersCollection.update(
        { id: 1 },
        {
          firstName: "Jye 2",
          age: { $inc: 10 },
        }
      );
      const rawUser = await usersCollection.retrieveOne({
        where: {
          id: 1,
        },
        returnRaw: true,
      });
      expect(rawUser).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _wc: 2,
        _c: testCollectionName,
        id: 1,
        firstName: "Jye 2",
        lastName: "Lewis",
        age: 40,
        isActivated: true,
      });
    });
  });

  describe("Delete", () => {
    it.concurrent("Can delete items", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
      });

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });

      const user1 = await usersCollection.retrieveOne({ where: { id: 1 } });
      expect(user1).not.toBeNull();

      await usersCollection.delete({ id: 1 });

      const user2 = await usersCollection.retrieveOne({ where: { id: 1 } });
      expect(user2).toBeNull();
    });

    it.concurrent("Handles deleting an item that doesn't exist", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
      });

      await expect(usersCollection.delete({ id: 1 })).resolves.not.toThrow();
    });

    it.concurrent("Runs onDelete extension handlers", async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      let deleteHandlerCalled = false;
      class DeleteExt extends DFBaseExtension<any> {
        async onDelete(key: Partial<any>) {
          expect(key).toEqual({ id: 1 });
          deleteHandlerCalled = true;
        }
      }

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
        extensions: [new DeleteExt()],
      });

      await usersCollection.insert({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
      });

      await usersCollection.delete({ id: 1 });

      expect(deleteHandlerCalled).toEqual(true);
    });
  });

  describe("Retrieve", () => {
    // annoying trick for prepping concurrent tests
    const thingsCollectionProm = (async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const thingsCollection = table.createCollection<Thing>({
        name: `things`,
        partitionKey: "groupId",
        sortKey: "thingId",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "byThingId",
            dynamoIndex: "GSI1",
            partitionKey: "thingId",
            sortKey: "groupId",
          }),
        ],
      });

      // insert test data
      await Promise.all([
        thingsCollection.insert({ groupId: 1, thingId: 1 }),
        thingsCollection.insert({ groupId: 1, thingId: 2 }),
        thingsCollection.insert({ groupId: 1, thingId: 3 }),
        thingsCollection.insert({ groupId: 2, thingId: 1 }),
        thingsCollection.insert({ groupId: 2, thingId: 2 }),
        thingsCollection.insert({ groupId: 2, thingId: 3 }),
      ]);

      return thingsCollection;
    })();

    describe("Basic operations", () => {
      it.concurrent("Can retrieveMany", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 1 },
          { groupId: 1, thingId: 2 },
          { groupId: 1, thingId: 3 },
        ]);
      });

      it.concurrent("retrieveMany respects item limit", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
          },
          limit: 2,
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 1 },
          { groupId: 1, thingId: 2 },
        ]);
      });

      it.concurrent(
        "retrieveOne returns [] if no items are found",
        async () => {
          const thingsCollection = await thingsCollectionProm;
          const things = await thingsCollection.retrieveMany({
            where: {
              groupId: 999,
            },
          });
          expect(things).toEqual([]);
        }
      );

      it.concurrent("Can retrieveMany from secondary index", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          index: "byThingId",
          where: {
            thingId: 2,
          },
        });

        expect(things).toEqual([
          { groupId: 1, thingId: 2 },
          { groupId: 2, thingId: 2 },
        ]);
      });

      it.concurrent(
        "Throws if no extensions can handle this index",
        async () => {
          const thingsCollection = await thingsCollectionProm;
          await expect(
            thingsCollection.retrieveMany({
              index: "miscIndex",
              where: {
                thingId: 2,
              },
            })
          ).rejects.toThrow(
            "No extensions available to handle querying by index 'miscIndex'"
          );
        }
      );

      it.concurrent("Can retrieveOne with specific key", async () => {
        const thingsCollection = await thingsCollectionProm;
        const thing = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
            thingId: 2,
          },
        });
        expect(thing).toEqual({ groupId: 1, thingId: 2 });
      });

      it.concurrent("Can retrieveOne from many items", async () => {
        const thingsCollection = await thingsCollectionProm;
        const thing = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
          },
        });
        expect(thing).toEqual({ groupId: 1, thingId: 1 });
      });

      it.concurrent(
        "retrieveOne returns null if item is not found",
        async () => {
          const thingsCollection = await thingsCollectionProm;
          const thing = await thingsCollection.retrieveOne({
            where: {
              groupId: 999,
            },
          });
          expect(thing).toEqual(null);
        }
      );

      it.concurrent("Can retrieveOne from secondary index", async () => {
        const thingsCollection = await thingsCollectionProm;
        const thing = await thingsCollection.retrieveOne({
          index: "byThingId",
          where: {
            thingId: 2,
          },
        });
        expect(thing).toEqual({ groupId: 1, thingId: 2 });
      });

      it.concurrent("Retrieves with sort direction specified", async () => {
        const thingsCollection = await thingsCollectionProm;
        const thingsAsc = await thingsCollection.retrieveMany({
          where: {
            groupId: 2,
          },
          sort: "ASC",
        });
        expect(thingsAsc).toEqual([
          { groupId: 2, thingId: 1 },
          { groupId: 2, thingId: 2 },
          { groupId: 2, thingId: 3 },
        ]);

        const thingsDesc = await thingsCollection.retrieveMany({
          where: {
            groupId: 2,
          },
          sort: "DESC",
        });
        expect(thingsDesc).toEqual([
          { groupId: 2, thingId: 3 },
          { groupId: 2, thingId: 2 },
          { groupId: 2, thingId: 1 },
        ]);
      });
    });

    describe("Retrieve various query expressions", () => {
      // doesn't need to be comprehensive, all the query expressions are tested in generateQueryExpression.test.ts
      it.concurrent("multi eq", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: 3,
          },
        });
        expect(things).toEqual([{ groupId: 1, thingId: 3 }]);
      });

      it.concurrent("$gt", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: { $gt: 1 },
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 2 },
          { groupId: 1, thingId: 3 },
        ]);
      });

      it.concurrent("$gte", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: { $gte: 2 },
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 2 },
          { groupId: 1, thingId: 3 },
        ]);
      });

      it.concurrent("$lt", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: { $lt: 3 },
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 1 },
          { groupId: 1, thingId: 2 },
        ]);
      });

      it.concurrent("$lte", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: { $lte: 2 },
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 1 },
          { groupId: 1, thingId: 2 },
        ]);
      });

      it.concurrent("$betweenIncl", async () => {
        const thingsCollection = await thingsCollectionProm;
        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: { $betweenIncl: [2, 3] },
          },
        });
        expect(things).toEqual([
          { groupId: 1, thingId: 2 },
          { groupId: 1, thingId: 3 },
        ]);
      });
    });

    describe("Processes items before returning", () => {
      it.concurrent("Returns raw row if requested", async () => {
        const thingsCollection = await thingsCollectionProm;

        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: 3,
          },
          returnRaw: true,
        });
        expect(things).toEqual([
          {
            _PK: expect.any(String),
            _SK: expect.any(String),
            _GSI1PK: expect.any(String),
            _GSI1SK: expect.any(String),
            _c: thingsCollection.config.name,
            _wc: 1,
            groupId: 1,
            thingId: 3,
          },
        ]);
      });

      it.concurrent("Strips metadata", async () => {
        const thingsCollection = await thingsCollectionProm;

        const things = await thingsCollection.retrieveMany({
          where: {
            groupId: 1,
            thingId: 3,
          },
        });

        expect(things).toEqual([{ groupId: 1, thingId: 3 }]);
      });

      it.concurrent("Runs migration if required", async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        let migrationsRun = 0;
        const migrationExtension = new DFMigrationExt<
          Thing & { sum: null | number }
        >({
          version: 1,
          migrateEntity: (version, entity: any) => {
            migrationsRun++;

            if (version === 1) {
              // migrate version 1 -> 2
              // set the sum property
              return {
                sum: entity.groupId + entity.thingId,
              };
            }

            return {};
          },
        });

        const thingsCollection = table.createCollection<
          Thing & { sum: null | number }
        >({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [migrationExtension],
        });

        expect(migrationsRun).toEqual(0);

        // insert test data (as version 1)
        await Promise.all([
          thingsCollection.insert({ groupId: 1, thingId: 1, sum: null }),
          thingsCollection.insert({ groupId: 1, thingId: 2, sum: null }),
          thingsCollection.insert({ groupId: 1, thingId: 3, sum: null }),
          thingsCollection.insert({ groupId: 2, thingId: 1, sum: null }),
          thingsCollection.insert({ groupId: 2, thingId: 2, sum: null }),
          thingsCollection.insert({ groupId: 2, thingId: 3, sum: null }),
        ]);

        expect(migrationsRun).toEqual(0);

        await expect(
          thingsCollection.retrieveMany({
            where: {
              groupId: 1,
              thingId: 3,
            },
          })
        ).resolves.toEqual([{ groupId: 1, thingId: 3, sum: null }]);

        expect(migrationsRun).toEqual(0);

        // bump our version number to version 2, to trigger some migrations
        migrationExtension.config.version = 2;

        await expect(
          thingsCollection.retrieveMany({
            where: {
              groupId: 1,
              thingId: 3,
            },
          })
        ).resolves.toEqual([{ groupId: 1, thingId: 3, sum: 4 }]);

        expect(migrationsRun).toEqual(1);

        // fetching again shouldn't re-run the migration
        await expect(
          thingsCollection.retrieveMany({
            where: {
              groupId: 1,
              thingId: 3,
            },
          })
        ).resolves.toEqual([{ groupId: 1, thingId: 3, sum: 4 }]);

        expect(migrationsRun).toEqual(1);
      });

      it.concurrent("Runs postRetrieve on all extensions", async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        let postRetrievesRun = 0;
        class MockExtension extends DFBaseExtension<any> {
          public postRetrieve(entity: any): void | Promise<void> {
            postRetrievesRun++;
          }
        }

        const thingsCollection = table.createCollection<Thing>({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [
            new MockExtension(),
            new MockExtension(),
            new MockExtension(),
          ],
        });

        expect(postRetrievesRun).toEqual(0);

        // insert test data
        await Promise.all([
          thingsCollection.insert({ groupId: 1, thingId: 1 }),
          thingsCollection.insert({ groupId: 1, thingId: 2 }),
          thingsCollection.insert({ groupId: 1, thingId: 3 }),
          thingsCollection.insert({ groupId: 2, thingId: 1 }),
          thingsCollection.insert({ groupId: 2, thingId: 2 }),
          thingsCollection.insert({ groupId: 2, thingId: 3 }),
        ]);

        expect(postRetrievesRun).toEqual(3 * 6);
        postRetrievesRun = 0;

        await expect(
          thingsCollection.retrieveMany({
            where: {
              groupId: 1,
              thingId: 3,
            },
          })
        ).resolves.toEqual([{ groupId: 1, thingId: 3 }]);

        expect(postRetrievesRun).toEqual(3 * 1);
        postRetrievesRun = 0;

        await expect(
          thingsCollection.retrieveMany({
            where: {
              groupId: 1,
            },
          })
        ).resolves.toEqual(expect.any(Array));

        expect(postRetrievesRun).toEqual(3 * 3);
      });
    });

    describe("Pagination", () => {
      it.concurrent("Can retrieve multiple pages of items", async () => {
        const thingsCollection = await thingsCollectionProm;

        const { items: items1, lastEvaluatedKey: lastEvaluatedKey1 } =
          await thingsCollection.retrieveManyWithPagination({
            where: {
              groupId: 1,
            },
            limit: 1,
          });

        expect(items1).toEqual([{ groupId: 1, thingId: 1 }]);
        expect(lastEvaluatedKey1).not.toBeUndefined();

        const { items: items2, lastEvaluatedKey: lastEvaluatedKey2 } =
          await thingsCollection.retrieveManyWithPagination({
            where: {
              groupId: 1,
            },
            limit: 1,
            exclusiveStartKey: lastEvaluatedKey1,
          });

        expect(items2).toEqual([{ groupId: 1, thingId: 2 }]);
        expect(lastEvaluatedKey2).not.toBeUndefined();

        const { items: items3 } =
          await thingsCollection.retrieveManyWithPagination({
            where: {
              groupId: 1,
            },
            limit: 1,
            exclusiveStartKey: lastEvaluatedKey2,
          });

        expect(items3).toEqual([{ groupId: 1, thingId: 3 }]);

        // the spec says this should be undefined on the last page
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseElements
        // however DDBLocal returns the last key of the last page!
        // expect(lastEvaluatedKey3).toBeUndefined();
      });
    });

    describe("Batch get", () => {
      it.concurrent("Can retrieveBatch", async () => {
        const thingsCollection = await thingsCollectionProm;

        const { items, unprocessedKeys } = await thingsCollection.retrieveBatch(
          [
            {
              groupId: 1,
              thingId: 1,
            },
            {
              groupId: 2,
              thingId: 2,
            },
            {
              groupId: 2,
              thingId: 3,
            },
          ]
        );

        expect(unprocessedKeys).toEqual([]);
        expect(items).toHaveLength(3);

        // ordering is not guaranteed
        expect(items).toEqual(
          expect.arrayContaining([
            { groupId: 1, thingId: 1 },
            { groupId: 2, thingId: 2 },
            { groupId: 2, thingId: 3 },
          ])
        );
      });

      it.concurrent("Can retrieveBatch with single item", async () => {
        const thingsCollection = await thingsCollectionProm;

        const { items, unprocessedKeys } = await thingsCollection.retrieveBatch(
          [
            {
              groupId: 1,
              thingId: 1,
            },
          ]
        );

        expect(unprocessedKeys).toEqual([]);
        expect(items).toHaveLength(1);

        expect(items).toEqual(
          expect.arrayContaining([{ groupId: 1, thingId: 1 }])
        );
      });
    });
  });

  describe("migrateEntityWithMetadata", () => {
    it.concurrent(
      "Runs all migration functions and persists item",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const migrationExtension = new DFMigrationExt<
          Thing & { sum: null | number }
        >({
          version: 1,
          migrateEntity: (version, entity: any) => {
            if (version === 1) {
              // migrate version 1 -> 2
              // set the sum property
              return {
                sum: entity.groupId + entity.thingId,
              };
            }

            return {};
          },
        });

        const thingsCollection = table.createCollection<
          Thing & { sum: null | number }
        >({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [migrationExtension],
        });

        await thingsCollection.insert({ groupId: 1, thingId: 1, sum: null });
        const insertedThing: any = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
            thingId: 1,
          },
          returnRaw: true,
        });

        const migratedEntity = await thingsCollection.migrateEntityWithMetadata(
          {
            _PK: insertedThing._PK,
            _SK: insertedThing._SK,
            _c: insertedThing._SK,
            _wc: insertedThing._wc,
            _v: insertedThing._v,
            groupId: 1,
            thingId: 1,
          }
        );
        expect(migratedEntity).toEqual({
          groupId: 1,
          thingId: 1,
          sum: 2,
        });
      }
    );

    it.concurrent(
      "Handles item being deleted while migration is in progress",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        const migrationExtension = new DFMigrationExt<Thing>({
          version: 1,
          migrateEntity: async (version, entity: any) => {
            // oopsie someone is trying to delete this item during a slow migration process
            // (wouldn't usually literally happen in the migration function, but maybe another web-request came in)
            await table.client.delete({
              TableName: table.tableName,
              Key: {
                _PK: entity._PK,
                _SK: entity._SK,
              },
            });

            return {};
          },
        });

        const thingsCollection = table.createCollection<Thing>({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [migrationExtension],
        });

        await thingsCollection.insert({ groupId: 1, thingId: 1 });
        const insertedThing: any = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
            thingId: 1,
          },
          returnRaw: true,
        });

        await expect(
          thingsCollection.migrateEntityWithMetadata({
            _PK: insertedThing._PK,
            _SK: insertedThing._SK,
            _c: insertedThing._SK,
            _wc: insertedThing._wc,
            _v: insertedThing._v,
            groupId: 1,
            thingId: 1,
          })
        ).rejects.toThrowError(
          "Item was deleted while migration was in progress, migration cancelled"
        );
      }
    );

    it.concurrent(
      "Can migrate while entities are being written to (locking works)",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());

        let numMigrationsRun = 0;
        const migrationExtension = new DFMigrationExt<Thing & { num: number }>({
          version: 1,
          migrateEntity: async (version, entity: any) => {
            numMigrationsRun++;

            // oopsie someone is trying to delete this item during a slow migration process
            // (wouldn't usually literally happen in the migration function, but maybe another web-request came in)
            if (entity.num === 1) {
              // we expect this migration will fail because someone else wrote to the item
              await thingsCollection.update(
                { groupId: entity.groupId, thingId: entity.thingId },
                {
                  num: 2,
                }
              );
            }

            return {};
          },
        });

        const thingsCollection = table.createCollection<
          Thing & { num: number }
        >({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [migrationExtension],
        });

        expect(numMigrationsRun).toEqual(0);

        await thingsCollection.insert({ groupId: 1, thingId: 1, num: 1 });

        expect(numMigrationsRun).toEqual(0);

        const insertedThing: any = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
            thingId: 1,
          },
          returnRaw: true,
        });

        expect(numMigrationsRun).toEqual(0);

        const migratedEntity = await thingsCollection.migrateEntityWithMetadata(
          insertedThing
        );
        expect(migratedEntity).toEqual({
          groupId: 1,
          thingId: 1,
          num: 2,
        });

        // one migration should have run, but failed to commit because of the write, then the second succeeded
        expect(numMigrationsRun).toEqual(2);
      }
    );

    it.concurrent(
      "Throws if extension completes migration but still thinks the entity needs migration",
      async () => {
        let enabledBadExtensionBehaviour = false;
        class BadExt extends DFBaseExtension<any> {
          public entityRequiresMigration(entity: any): boolean {
            // always complain about the entity, even after we've been allowed to migrate it
            return enabledBadExtensionBehaviour;
          }

          public migrateEntity() {
            // no need to actually change anything
          }
        }

        const table = new DFTable(testDbConfigWithPrefix());
        const thingsCollection = table.createCollection<Thing>({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [new BadExt()],
        });

        await thingsCollection.insert({ groupId: 1, thingId: 1 });

        const insertedThing: any = await thingsCollection.retrieveOne({
          where: {
            groupId: 1,
            thingId: 1,
          },
          returnRaw: true,
        });

        enabledBadExtensionBehaviour = true;
        await expect(
          thingsCollection.migrateEntityWithMetadata(insertedThing)
        ).rejects.toThrow(
          "Extension BadExt still requires migration after migration was run"
        );
      }
    );

    it.concurrent(
      "Throws if extension completes migration but still thinks the entity needs migration (for insert)",
      async () => {
        class BadExt extends DFBaseExtension<any> {
          public entityRequiresMigration(entity: any): boolean {
            // always complain about the entity, even after we've been allowed to migrate it
            return true;
          }

          public migrateEntity() {
            // no need to actually change anything
          }
        }

        const table = new DFTable(testDbConfigWithPrefix());
        const thingsCollection = table.createCollection<Thing>({
          name: `things-migration`,
          partitionKey: "groupId",
          sortKey: "thingId",
          extensions: [new BadExt()],
        });

        await expect(
          thingsCollection.insert({ groupId: 1, thingId: 1 })
        ).rejects.toThrow(
          "Extension BadExt still requires migration after migration was run"
        );
      }
    );
  });
});
