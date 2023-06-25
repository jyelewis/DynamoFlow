import { DFDB } from "../DFDB.js";
import { testDbConfig } from "../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../testHelpers/genTestPrefix.js";
import { DFSecondaryIndexExt } from "../extensions/DFSecondaryIndexExt.js";

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
  describe("Insert", () => {
    it.concurrent("Can insert items", async () => {
      const db = new DFDB(testDbConfig);

      const usersCollection = db.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [],
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
        const db = new DFDB(testDbConfig);

        const usersCollection = db.createCollection<User>({
          name: `${genTestPrefix()}-user`,
          partitionKey: "id",
          extensions: [],
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
        const db = new DFDB(testDbConfig);

        const usersCollection = db.createCollection<User>({
          name: `${genTestPrefix()}-user`,
          partitionKey: "id",
          extensions: [],
        });

        const transaction = await usersCollection.insertTransaction({
          id: 1,
          firstName: "Jye",
          lastName: "Lewis",
          isActivated: true,
        });
        transaction.addSecondaryTransaction(
          await usersCollection.insertTransaction({
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
  });

  describe("Update", () => {
    it.concurrent("Can update items", async () => {
      const db = new DFDB(testDbConfig);

      const usersCollection = db.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [],
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
      "Throws if updating an item that already exists",
      async () => {
        const db = new DFDB(testDbConfig);

        const usersCollection = db.createCollection<User>({
          name: `${genTestPrefix()}-user`,
          partitionKey: "id",
          extensions: [],
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

    it("Can update multiple items within a transaction", async () => {
      const db = new DFDB(testDbConfig);

      const usersCollection = db.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [],
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

      const transaction = await usersCollection.updateTransaction(
        { id: 1 },
        {
          firstName: "Jye 2",
          age: { $inc: 10 },
        }
      );
      transaction.addSecondaryTransaction(
        await usersCollection.updateTransaction(
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
    });
  });

  describe("Retrieve", () => {
    // annoying trick for prepping concurrent tests
    const thingsCollectionProm = (async () => {
      const db = new DFDB(testDbConfig);
      const thingsCollection = db.createCollection<Thing>({
        name: `${genTestPrefix()}-things`,
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
    });

    describe("Retrieve various query expressions", () => {
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

      // TODO: test $beginsWith
      // TODO: test multiple sorts
    });
  });
});
