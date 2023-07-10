import { DFTable } from "../DFTable.js";
import { testDbConfig } from "../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../testHelpers/genTestPrefix.js";
import { DFUpdateOperation } from "../types/operations.js";
import { RETRY_TRANSACTION } from "../types/types.js";
import { setTimeout } from "timers/promises";
import { DFConditionalCheckFailedException } from "../errors/DFConditionalCheckFailedException.js";

describe("DFWriteTransaction", () => {
  describe("Basic single operations", () => {
    it.concurrent("Executes single write transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).toBeUndefined();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
      });
    });

    it.concurrent(
      "Executes single write transaction (with inc operation)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
            age: 10,
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye 2",
            lastName: "Lewis 2",
            age: { $inc: 1 },
          },
        });
        const updatedEntity = await transaction.commit();
        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });
      }
    );

    it.concurrent(
      "Executes single write transaction (with $setIfNotExists operation - doesn't exist)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye 2",
            lastName: "Lewis 2",
            age: { $setIfNotExists: 11 },
          },
        });
        const updatedEntity = await transaction.commit();
        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });
      }
    );

    it.concurrent(
      "Executes single write transaction (with $setIfNotExists operation - already exists)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
            age: 10,
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye 2",
            lastName: "Lewis 2",
            age: { $setIfNotExists: 11 },
          },
        });
        const updatedEntity = await transaction.commit();
        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 10, // shouldn't update, already exists
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 10,
        });
      }
    );

    it.concurrent(
      "Executes single write transaction (with $inc + $remove operation)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
            age: 10,
            isLegacyUser: true,
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye 2",
            lastName: "Lewis 2",
            age: { $inc: 1 },
            isLegacyUser: { $remove: true },
          },
        });
        const updatedEntity = await transaction.commit();
        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye 2",
          lastName: "Lewis 2",
          age: 11,
        });
      }
    );

    it.concurrent(
      "Executes single write transaction with condition + success handler (condition passes)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const preTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(preTestGet.Item).toBeUndefined();

        const successHandler = jest.fn((item: any) => {
          expect(item).toEqual({
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
            age: 1,
          });
        });
        const errorHandler = jest.fn();
        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            age: { $inc: 1 },
          },
          condition: {
            firstName: { $exists: false },
          },
          successHandlers: [successHandler],
          errorHandler,
        });
        await transaction.commit();

        expect(successHandler).toHaveBeenCalled();
        expect(errorHandler).not.toHaveBeenCalled();

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
          age: 1,
        });
      }
    );

    it.concurrent(
      "Executes single write transaction with condition + error handler (condition fails)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const preTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(preTestGet.Item).toBeUndefined();

        const successHandler = jest.fn();
        const errorHandler = jest.fn(() => {
          throw new Error("Custom message from error handler");
        });
        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
          },
          condition: {
            firstName: { $exists: true },
          },
          successHandlers: [successHandler],
          errorHandler,
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Custom message from error handler"
        );

        expect(successHandler).not.toHaveBeenCalled();
        expect(errorHandler).toHaveBeenCalled();

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual(undefined);
      }
    );

    it.concurrent("Executes single delete transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      await table.client.put({
        TableName: table.tableName,
        Item: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
        },
      });

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).not.toBeUndefined();

      const transaction = table.createTransaction({
        type: "Delete",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      const transRes = await transaction.commit();
      expect(transRes).toEqual(null);

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toBeUndefined();
    });

    it.concurrent(
      "Executes single delete transaction with condition (condition fails)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
          },
        });

        const preTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(preTestGet.Item).not.toBeUndefined();

        const transaction = table.createTransaction({
          type: "Delete",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          condition: {
            firstName: "Joe",
          },
        });
        await expect(transaction.commit()).rejects.toThrow(
          "Conditional check failed"
        );

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        // item should not have been deleted (transaction failed)
        expect(postTestGet.Item).not.toBeUndefined();
      }
    );

    it.concurrent("Throws if operation type is unknown", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "UnknownThing" as any,
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });

      await expect(transaction.commit()).rejects.toThrow(
        "Unknown operation type"
      );
    });
  });

  describe("set/list/map single operations", () => {
    it.concurrent("Stores object", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).toBeUndefined();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          address: {
            street: "123 Fake St",
            city: "Fakeville",
          },
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          street: "123 Fake St",
          city: "Fakeville",
        },
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          street: "123 Fake St",
          city: "Fakeville",
        },
      });
    });

    it.concurrent("Updates object properties", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).toBeUndefined();

      // create item
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            address: {
              city: "Faketown",
              postcode: 5222,
            },
          },
        })
        .commit();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          // replace address
          address: {
            city: "Fakevile",
            postcode: 6543,
          },
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          city: "Fakevile",
          postcode: 6543,
        },
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          city: "Fakevile",
          postcode: 6543,
        },
      });
    });

    it.concurrent("Stores list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          addresses: [{ street: "123 Fake St", city: "Fakeville" }],
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [{ street: "123 Fake St", city: "Fakeville" }],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [{ street: "123 Fake St", city: "Fakeville" }],
      });
    });

    it.concurrent("Replaces list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            addresses: [{ street: "123 Fake St", city: "Fakeville" }],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          addresses: [
            { street: "Other street 1", city: "Mo town" },
            { street: "Other street 2", city: "Mo town" },
          ],
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2", city: "Mo town" },
        ],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2", city: "Mo town" },
        ],
      });
    });

    it.concurrent("Appends to list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            addresses: [{ street: "123 Fake St", city: "Fakeville" }],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          addresses: {
            $appendItemsToList: [
              { street: "Other street 1", city: "Mo town" },
              { street: "Other street 2", city: "Mo town" },
            ],
          },
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "123 Fake St", city: "Fakeville" },
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2", city: "Mo town" },
        ],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "123 Fake St", city: "Fakeville" },
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2", city: "Mo town" },
        ],
      });
    });

    it.concurrent("Deletes from list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            addresses: [
              { street: "Other street 1", city: "Mo town" },
              { street: "Other street 2", city: "Mo town" },
              { street: "Other street 3", city: "Mo town" },
            ],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          "addresses[0]": { $remove: true },
          "addresses[2]": { $remove: true },
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [{ street: "Other street 2", city: "Mo town" }],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [{ street: "Other street 2", city: "Mo town" }],
      });
    });

    it.concurrent("Creates & modifies lists literals", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: [2, 4, 6, 8, 28],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          favouriteNumbers: {
            $appendItemsToList: [23, 100],
          },
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [2, 4, 6, 8, 28, 23, 100],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [2, 4, 6, 8, 28, 23, 100],
      });
    });

    it.concurrent("Updates literal within list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: [2, 4, 6, 8, 28],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          // legacy API
          // favouriteNumbers: {
          //   $replaceListItems: {
          //     0: 0, // replace 2
          //     2: 12, // replace 6
          //   },
          // },
          "favouriteNumbers[0]": 0, // replace 2
          "favouriteNumbers[2]": 12, // replace 6
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [0, 4, 12, 8, 28],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [0, 4, 12, 8, 28],
      });
    });

    it.concurrent("Updates object within list", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with list
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            addresses: [
              { street: "Other street 1", city: "Mo town" },
              { street: "Other street 2", city: "Mo town" },
              { street: "Other street 3", city: "Mo town" },
            ],
          },
        })
        .commit();

      // replace list
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          "addresses[1].street": "Other street 2 modified",
        },
      });
      const updatedEntity = await transaction.commit();

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2 modified", city: "Mo town" },
          { street: "Other street 3", city: "Mo town" },
        ],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          { street: "Other street 1", city: "Mo town" },
          { street: "Other street 2 modified", city: "Mo town" },
          { street: "Other street 3", city: "Mo town" },
        ],
      });
    });

    it.concurrent("Stores set", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with set
      const createdEntity = await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: new Set([2, 4, 6, 8, 28]),
          },
        })
        .commit();

      expect(createdEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 6, 8, 28]),
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 6, 8, 28]),
      });
    });

    it.concurrent("Adds to set (exists and not exists)", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with set
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: new Set([2, 4, 6, 8, 28]),
          },
        })
        .commit();

      const createdEntity = await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: {
              $addItemsToSet: new Set([100, 128, 6]),
            },
          },
        })
        .commit();

      expect(createdEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 6, 8, 28, 100, 128]),
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 6, 8, 28, 100, 128]),
      });
    });

    it.concurrent("Removes from set (exists and not exists)", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // create document with set
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: new Set([2, 4, 6, 8, 28]),
          },
        })
        .commit();

      const createdEntity = await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: {
              $removeItemsFromSet: new Set([100, 128, 6]),
            },
          },
        })
        .commit();

      expect(createdEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 8, 28]),
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: new Set([2, 4, 8, 28]),
      });
    });

    it.concurrent("Stores list with complex properties", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
          addresses: [
            {
              street: "123 Fake St",
              city: "Fakeville",
              postcode: 1234,
              deliveryDays: new Set(["MON", "WED", "THURS"]),
              occupantNames: ["Jane Doe", "John Smith"],
              occupantAges: [30, 32],
              driverRatings: {
                sam: 5,
                blake: 4.5,
              },
            },
          ],
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          {
            street: "123 Fake St",
            city: "Fakeville",
            postcode: 1234,
            deliveryDays: new Set(["MON", "WED", "THURS"]),
            occupantNames: ["Jane Doe", "John Smith"],
            occupantAges: [30, 32],
            driverRatings: {
              sam: 5,
              blake: 4.5,
            },
          },
        ],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        addresses: [
          {
            street: "123 Fake St",
            city: "Fakeville",
            postcode: 1234,
            deliveryDays: new Set(["MON", "WED", "THURS"]),
            occupantNames: ["Jane Doe", "John Smith"],
            occupantAges: [30, 32],
            driverRatings: {
              sam: 5,
              blake: 4.5,
            },
          },
        ],
      });
    });

    it.concurrent("Sets value of nested object", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).toBeUndefined();

      // create item
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            address: {
              city: "Faketown",
            },
          },
        })
        .commit();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          lastName: "Lewis",
          // update city
          "address.city": "Fakeville",
          // add postcode
          "address.postcode": 3241,
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          city: "Fakeville",
          postcode: 3241,
        },
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        address: {
          city: "Fakeville",
          postcode: 3241,
        },
      });
    });

    it.concurrent("Sets value of nested list entry", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(preTestGet.Item).toBeUndefined();

      // create item
      await table
        .createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
            favouriteNumbers: [1, 2, 3],
          },
        })
        .commit();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          lastName: "Lewis",
          // Update item at index 1
          "favouriteNumbers[1]": 6,
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [1, 6, 3],
      });

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
        favouriteNumbers: [1, 6, 3],
      });
    });

    it.concurrent(
      "Performs operations on complex object properties",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const preTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(preTestGet.Item).toBeUndefined();

        // create item
        await table
          .createTransaction({
            type: "Update",
            key: {
              _PK: `${keyPrefix}USER#user1`,
              _SK: "USER#user1",
            },
            updateValues: {
              firstName: "Jye",
              lastName: "Lewis",
              details: {
                address: {
                  city: "Faketown",
                  postcode: 3241,
                  deliveryDays: new Set(["Monday", "Tuesday"]),
                  listOfNumbers: [1, 2, 3],
                },
              },
            },
          })
          .commit();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            lastName: "Lewis",
            // update city
            "details.address.city": "Fakeville",
            // inc postcode
            "details.address.postcode": { $inc: 5 },
            "details.address.listOfNumbers[1]": { $inc: 5 },
            "details.address.deliveryDays": {
              $addItemsToSet: new Set(["Thursday"]),
            },
          },
        });
        const updatedEntity = await transaction.commit();
        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
          details: {
            address: {
              city: "Fakeville",
              postcode: 3246,
              deliveryDays: new Set(["Monday", "Tuesday", "Thursday"]),
              listOfNumbers: [1, 7, 3],
            },
          },
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
          details: {
            address: {
              city: "Fakeville",
              postcode: 3246,
              deliveryDays: new Set(["Monday", "Tuesday", "Thursday"]),
              listOfNumbers: [1, 7, 3],
            },
          },
        });
      }
    );

    it.concurrent(
      "Throws if trying to use an update key starting with an index expression",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            lastName: "Lewis",
            // invalid update expression
            "[0]": "test",
          },
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Invalid key, cannot start index lookup"
        );
      }
    );

    it.concurrent("Throws if update expression is not recognised", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          lastName: { $unknownOperation: 123 },
        },
      });

      await expect(transaction.commit()).rejects.toThrow(
        `Invalid update operation: {"$unknownOperation":123}`
      );
    });
  });

  describe("Basic multiple operations", () => {
    it.concurrent("Executes write+write transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const preTestGet1 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      const preTestGet2 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
      });
      expect(preTestGet1.Item).toBeUndefined();
      expect(preTestGet2.Item).toBeUndefined();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
        },
      });
      transaction.addSecondaryOperation({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });
      const updatedEntity = await transaction.commit();
      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
      });

      const postTestGet1 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      const postTestGet2 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
      });
      expect(postTestGet1.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
      });
      expect(postTestGet2.Item).toEqual({
        _PK: `${keyPrefix}USER#user2`,
        _SK: "USER#user2",
        firstName: "Joe",
        lastName: "Bot",
      });
    });

    it.concurrent(
      "Executes write+write with embedded condition transaction (condition fails)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const preTestGet1 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        const preTestGet2 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
        });
        expect(preTestGet1.Item).toBeUndefined();
        expect(preTestGet2.Item).toBeUndefined();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
          },
        });
        transaction.addSecondaryOperation({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // condition expression to ensure this item already exists (it doesn't, should fail)
          condition: {
            _PK: { $exists: true },
          },
        });

        await expect(transaction.commit()).rejects.toThrow(
          "ConditionalCheckFailed"
        );

        const postTestGet1 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        const postTestGet2 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
        });
        expect(postTestGet1.Item).toEqual(undefined);
        expect(postTestGet2.Item).toEqual(undefined);
      }
    );

    it.concurrent(
      "Executes write+write and retrieves value for both return handlers",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const preTestGet1 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        const preTestGet2 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
        });
        expect(preTestGet1.Item).toBeUndefined();
        expect(preTestGet2.Item).toBeUndefined();

        const successHandler1 = jest.fn();
        const successHandler2 = jest.fn();
        const successHandler3 = jest.fn();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Jye",
            lastName: "Lewis",
          },
          successHandlers: [successHandler1],
        });
        transaction.addSecondaryOperation({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          successHandlers: [successHandler2, successHandler3],
        });

        const returnedItem1 = await transaction.commit();

        // verify our handlers were run with appropriate callbacks
        expect(successHandler1).toHaveBeenCalledWith({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
        });
        expect(successHandler2).toHaveBeenCalledWith({
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
          firstName: "Joe",
          lastName: "Bot",
        });

        // check that item 1 was re-used, and we didn't retrieve it twice
        expect(returnedItem1 === successHandler1.mock.calls[0][0]).toBe(true);

        // check that item 2 was re-used, and we didn't retrieve it twice
        expect(
          successHandler2.mock.calls[0][0] === successHandler3.mock.calls[0][0]
        ).toBe(true);

        const postTestGet1 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        const postTestGet2 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
        });
        expect(postTestGet1.Item).not.toEqual(undefined);
        expect(postTestGet2.Item).not.toEqual(undefined);
      }
    );

    it.concurrent("Executes delete+delete transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // add some test data
      await table.client.batchWrite({
        RequestItems: {
          [table.tableName]: [
            {
              PutRequest: {
                Item: {
                  _PK: `${keyPrefix}USER#user1`,
                  _SK: "USER#user1",
                  firstName: "Jye",
                  lastName: "Lewis",
                },
              },
            },
            {
              PutRequest: {
                Item: {
                  _PK: `${keyPrefix}USER#user2`,
                  _SK: "USER#user2",
                  firstName: "Joe",
                  lastName: "Bot",
                },
              },
            },
          ],
        },
      });

      const transaction = table.createTransaction({
        type: "Delete",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      transaction.addSecondaryOperation({
        type: "Delete",
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
      });

      await transaction.commit();

      // check both items were deleted
      const postTestGet1 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      const postTestGet2 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
      });
      expect(postTestGet1.Item).toEqual(undefined);
      expect(postTestGet2.Item).toEqual(undefined);
    });

    it.concurrent("Executes write+delete transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // add some test data
      await table.client.put({
        TableName: table.tableName,
        Item: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
        },
      });

      // swap out user 1 for user 2, as a transaction
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });
      transaction.addSecondaryOperation({
        type: "Delete",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });

      const returnedItem = await transaction.commit();
      expect(returnedItem).toEqual({
        _PK: `${keyPrefix}USER#user2`,
        _SK: "USER#user2",
        firstName: "Joe",
        lastName: "Bot",
      });

      // check user 1 was deleted & user 2 was created
      const postTestGet1 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      const postTestGet2 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
      });
      expect(postTestGet1.Item).toEqual(undefined);
      expect(postTestGet2.Item).not.toEqual(undefined);
    });

    it.concurrent("Executes write+conditionCheck transaction", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      // update user 2 (primary) delete user 1 (secondary) and condition check user 3 (secondary)
      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });
      transaction.addSecondaryOperation({
        type: "ConditionCheck",
        key: {
          _PK: `${keyPrefix}USER#user3`,
          _SK: "USER#user3",
        },
        condition: {
          _PK: { $exists: false },
        },
      });

      const returnedItem = await transaction.commit();
      expect(returnedItem).toEqual({
        _PK: `${keyPrefix}USER#user2`,
        _SK: "USER#user2",
        firstName: "Joe",
        lastName: "Bot",
      });

      // check user 1 was deleted & user 2 was created
      const postTestGet1 = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user3`,
          _SK: "USER#user3",
        },
      });
      expect(postTestGet1.Item).toEqual(undefined);
    });

    it.concurrent(
      "Executes write+conditionCheck transaction (condition check fails)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        // update user 2 (primary) delete user 1 (secondary) and condition check user 3 (secondary)
        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
        });
        transaction.addSecondaryOperation({
          type: "ConditionCheck",
          key: {
            _PK: `${keyPrefix}USER#user3`,
            _SK: "USER#user3",
          },
          condition: {
            _PK: { $exists: true },
          },
          errorHandler: (e) => {
            expect(e).toBeInstanceOf(DFConditionalCheckFailedException);

            throw new Error("Conditional check caught and re-thrown");
          },
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Conditional check caught and re-thrown"
        );
      }
    );

    it.concurrent(
      "Executes write+delete+conditionCheck transaction",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        // add some test data
        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
          },
        });

        // update user 2 (primary) delete user 1 (secondary) and condition check user 3 (secondary)
        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
        });
        transaction.addSecondaryOperation({
          type: "Delete",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        transaction.addSecondaryOperation({
          type: "ConditionCheck",
          key: {
            _PK: `${keyPrefix}USER#user3`,
            _SK: "USER#user3",
          },
          condition: {
            _PK: { $exists: false },
          },
        });

        const returnedItem = await transaction.commit();
        expect(returnedItem).toEqual({
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
          firstName: "Joe",
          lastName: "Bot",
        });

        // check user 1 was deleted & user 2 was created
        const postTestGet1 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        const postTestGet2 = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user3`,
            _SK: "USER#user3",
          },
        });
        expect(postTestGet1.Item).toEqual(undefined);
        expect(postTestGet2.Item).toEqual(undefined);
      }
    );

    it.concurrent(
      "Throws if error handler doesn't throw or request a re-try (multiple items)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // this condition will never pass without something externally changing
          // should cause is to re-try indefinitely
          condition: {
            _PK: { $exists: true },
          },
          errorHandler: (() => {
            // do nothing
          }) as any, // ts shouldn't let us actually write an errorHandler like this
        });
        transaction.addSecondaryOperation({
          type: "ConditionCheck",
          key: {
            _PK: `${keyPrefix}USER#user2`,
            _SK: "USER#user2",
          },
          condition: {
            _PK: { $exists: false },
          },
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Transaction operation error handler returned invalid response. Error handlers must throw an error themselves, or return RETRY_TRANSACTION"
        );
      }
    );

    it.concurrent("Throws if operation type is unknown", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });
      transaction.addSecondaryOperation({
        type: "UnknownThing" as any,
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
        condition: {
          _PK: { $exists: false },
        },
      });

      await expect(transaction.commit()).rejects.toThrow(
        "Unknown operation type"
      );
    });

    it.concurrent(
      "Throws if .commitWithReturn() is called on a transaction where the primary operation is not 'update'",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const transaction = table.createTransaction({
          type: "Delete",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        await expect(transaction.commitWithReturn()).rejects.toThrowError(
          "Cannot call commitWithReturn() on a transaction with no primary operation of type 'Update'"
        );
      }
    );
  });

  describe("Pre-commit handlers", () => {
    it.concurrent("Runs pre-commit handlers for single operation", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const eventLog: string[] = [];

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
        },
      });
      transaction.addPreCommitHandler(async () => {
        eventLog.push("pre-commit handler 1");

        // modify the transaction
        transaction.primaryUpdateOperation.updateValues.firstName += "2";
      });
      transaction.addPreCommitHandler(async () => {
        eventLog.push("pre-commit handler 2");
        // do something async
        await setTimeout(0);
      });
      eventLog.push("pre-commit");
      const updatedEntity = await transaction.commit();
      eventLog.push("post-commit");

      expect(updatedEntity).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye2",
        lastName: "Lewis",
      });
      expect(eventLog).toEqual([
        "pre-commit",
        "pre-commit handler 1",
        "pre-commit handler 2",
        "post-commit",
      ]);

      const postTestGet = await table.client.get({
        TableName: table.tableName,
        Key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
      });
      expect(postTestGet.Item).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye2", // changed by the pre-commit hook
        lastName: "Lewis",
      });
    });

    it.todo("Runs pre-commit handlers for a multiple operation");

    it.todo("Runs pre-commit on re-try");
  });

  describe("Transaction error handling & retrying", () => {
    it.concurrent(
      "Re-tries transaction if error handler returns RETRY_TRANSACTION (single item)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        // set up some data
        // this will mean we need to label our user "user2" with a transaction auto retry
        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // only if this ID isn't taken
          condition: {
            _PK: { $exists: false },
          },
          errorHandler: (err, op: DFUpdateOperation) => {
            const currentUserNum = parseInt(
              (op.key as any)._PK.split("USER#user")[1],
              10
            );

            (op.key as any) = {
              _PK: `${keyPrefix}USER#user${currentUserNum + 1}`,
              _SK: `USER#user${currentUserNum + 1}`,
            };

            return RETRY_TRANSACTION;
          },
        });

        const updatedEntity = await transaction.commit();

        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
          firstName: "Joe",
          lastName: "Bot",
        });

        const postTestGet = await table.client.get({
          TableName: table.tableName,
          Key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
        });
        expect(postTestGet.Item).toEqual({
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
          firstName: "Jye",
          lastName: "Lewis",
        });
      }
    );

    it.concurrent(
      "Throws if error handler doesn't throw or request a re-try",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // this condition will never pass without something externally changing
          // should cause is to re-try indefinitely
          condition: {
            _PK: { $exists: true },
          },
          errorHandler: (() => {
            // do nothing
          }) as any, // ts shouldn't let us actually write an errorHandler like this
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Transaction operation error handler returned invalid response. Error handlers must throw an error themselves, or return RETRY_TRANSACTION"
        );
      }
    );

    it.concurrent("Throws if too many re-tries are attempted", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
        // this condition will never pass without something externally changing
        // should cause is to re-try indefinitely
        condition: {
          _PK: { $exists: true },
        },
        errorHandler: () => {
          return RETRY_TRANSACTION;
        },
      });

      await expect(transaction.commit()).rejects.toThrow(
        "Write transaction failed: Max retries exceeded"
      );
    });

    it.concurrent(
      "Re-tries transaction if error handler returns RETRY_TRANSACTION (with secondary ops)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        // set up some data
        // this will mean we need to label our user "user2" with a transaction auto retry
        await table.client.put({
          TableName: table.tableName,
          Item: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
            firstName: "Jye",
            lastName: "Lewis",
          },
        });

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // only if this ID isn't taken
          condition: {
            _PK: { $exists: false },
          },
          errorHandler: (err, op: DFUpdateOperation) => {
            const currentUserNum = parseInt(
              (op.key as any)._PK.split("USER#user")[1],
              10
            );

            (op.key as any) = {
              _PK: `${keyPrefix}USER#user${currentUserNum + 1}`,
              _SK: `USER#user${currentUserNum + 1}`,
            };

            return RETRY_TRANSACTION;
          },
        });
        transaction.addSecondaryOperation({
          // condition should always succeed
          type: "ConditionCheck",
          key: {
            _PK: `${keyPrefix}USER#not_legacy_user`,
            _SK: "USER#not_legacy_user",
          },
          condition: {
            _PK: { $exists: false },
          },
        });

        const updatedEntity = await transaction.commit();

        expect(updatedEntity).toEqual({
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
          firstName: "Joe",
          lastName: "Bot",
        });
      }
    );

    it.concurrent(
      "Throws if too many re-tries are attempted (with secondaryOps)",
      async () => {
        const table = new DFTable(testDbConfig);
        const keyPrefix = genTestPrefix();

        const transaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: `${keyPrefix}USER#user1`,
            _SK: "USER#user1",
          },
          updateValues: {
            firstName: "Joe",
            lastName: "Bot",
          },
          // this condition will never pass without something externally changing
          // should cause is to re-try indefinitely
          condition: {
            _PK: { $exists: true },
          },
          errorHandler: () => {
            return RETRY_TRANSACTION;
          },
        });
        transaction.addSecondaryOperation({
          // condition should always succeed
          type: "ConditionCheck",
          key: {
            _PK: `${keyPrefix}USER#not_legacy_user`,
            _SK: "USER#not_legacy_user",
          },
          condition: {
            _PK: { $exists: false },
          },
        });

        await expect(transaction.commit()).rejects.toThrow(
          "Write transaction failed: Max retries exceeded"
        );
      }
    );
  });

  describe("Merging transactions", () => {
    it.concurrent("Merges transactions", async () => {
      const table = new DFTable(testDbConfig);
      const keyPrefix = genTestPrefix();

      const transaction1 = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user1`,
          _SK: "USER#user1",
        },
        updateValues: {
          firstName: "Jye",
          lastName: "Lewis",
        },
      });
      transaction1.addSecondaryOperation({
        type: "ConditionCheck",
        key: {
          _PK: `${keyPrefix}USEREMAIL#jye@gmail`,
          _SK: `USEREMAIL#jye@gmail`,
        },
        condition: {
          _PK: { $exists: false },
        },
      });

      const transaction2 = table.createTransaction({
        type: "Update",
        key: {
          _PK: `${keyPrefix}USER#user2`,
          _SK: "USER#user2",
        },
        updateValues: {
          firstName: "Joe",
          lastName: "Bot",
        },
      });
      transaction2.addSecondaryOperation({
        type: "ConditionCheck",
        key: {
          _PK: `${keyPrefix}USEREMAIL#joe@gmail`,
          _SK: `USEREMAIL#joe@gmail`,
        },
        condition: {
          _PK: { $exists: false },
        },
      });

      // merge transaction 2 into transaction 1
      transaction1.addSecondaryTransaction(transaction2);

      // check we have everything
      expect(transaction1.secondaryOperations.length).toEqual(3);

      const transRes = await transaction1.commit();
      expect(transRes).toEqual({
        _PK: `${keyPrefix}USER#user1`,
        _SK: "USER#user1",
        firstName: "Jye",
        lastName: "Lewis",
      });
    });

    it.todo("Runs pre-commit for all items in merged transaction");
  });
});
