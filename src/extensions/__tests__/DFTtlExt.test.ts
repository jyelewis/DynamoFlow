import { DFTtlExt } from "../DFTtlExt.js";
import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import { generateIndexStrings } from "../../utils/generateIndexStrings.js";
import { DFCollection } from "../../DFCollection.js";

interface CacheItem {
  partition: string;
  id: string;
  value: string;
  expiresAt: null | string;
}

// 7 days from now
const isoStringIn7Days = new Date(
  Date.now() + 1000 * 60 * 60 * 24 * 7
).toISOString();
const unixTimestampIn7Days = Math.floor(
  new Date(Date.now() + 1000 * 60 * 60 * 24 * 7).getTime() / 1000
);

const isoStringYesterday = new Date(
  Date.now() - 1000 * 60 * 60 * 24
).toISOString();
const unixTimestampYesterday = Math.floor(
  new Date(Date.now() - 1000 * 60 * 60 * 24).getTime() / 1000
);

describe("DFTtlExt", () => {
  describe("dateValueToDynamoTtl", () => {
    const ext = new DFTtlExt({
      expiresAtField: "expiresAt",
    });

    it("Converts ms timestamp", () => {
      expect(ext.dateValueToDynamoTtl(unixTimestampIn7Days * 1000)).toEqual(
        Math.floor(unixTimestampIn7Days)
      );
    });

    it("Converts ISO string timestamp", () => {
      expect(ext.dateValueToDynamoTtl(isoStringIn7Days)).toEqual(
        unixTimestampIn7Days
      );
    });

    it("Throws if value is not a valid date", () => {
      expect(() => ext.dateValueToDynamoTtl(null)).toThrowError(
        "Invalid date provided to DFTtlExt"
      );
      expect(() => ext.dateValueToDynamoTtl(undefined as any)).toThrowError(
        "Invalid date provided to DFTtlExt"
      );
      expect(() => ext.dateValueToDynamoTtl({})).toThrowError(
        "Invalid date provided to DFTtlExt"
      );
      expect(() => ext.dateValueToDynamoTtl([])).toThrowError(
        "Invalid date provided to DFTtlExt"
      );
      expect(() => ext.dateValueToDynamoTtl(Infinity)).toThrowError(
        "Invalid date provided to DFTtlExt"
      );
    });

    it("Throws if date is in the past", () => {
      expect(() => ext.dateValueToDynamoTtl(123)).toThrowError(
        "Date provided to DFTtlExt is in the past"
      );
    });
  });

  describe("Insert", () => {
    it.concurrent("Stores converted value in _ttl if provided", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "partition",
        sortKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      const insertedItem = await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });
      expect(insertedItem).toEqual({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          partition: "1",
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 1,
        _ttl: unixTimestampIn7Days,
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });
    });

    it.concurrent("Does not write _ttl if no expiry provided", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "partition",
        sortKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      const insertedItem = await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: null,
      });
      expect(insertedItem).toEqual({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: null,
      });

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          partition: "1",
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 1,
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: null,
      });
    });
  });

  describe("Update", () => {
    it.concurrent("Stores converted value in _ttl if provided", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "partition",
        sortKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: null,
      });

      await cacheItemsCollection.update(
        {
          partition: "1",
          id: "1",
        },
        {
          expiresAt: isoStringIn7Days,
        }
      );

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          partition: "1",
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 2,
        _ttl: unixTimestampIn7Days,
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });
    });

    it.concurrent("Does not update _ttl if expiry hasn't changed", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "partition",
        sortKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });

      await cacheItemsCollection.update(
        {
          partition: "1",
          id: "1",
        },
        {
          value: "bar",
        }
      );

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          partition: "1",
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 2,
        _ttl: unixTimestampIn7Days,
        partition: "1",
        id: "1",
        value: "bar",
        expiresAt: isoStringIn7Days,
      });
    });

    it.concurrent("Removes _ttl if expiry field is set removed", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "partition",
        sortKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });

      await cacheItemsCollection.update(
        {
          partition: "1",
          id: "1",
        },
        {
          expiresAt: { $remove: true },
        }
      );

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          partition: "1",
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 2,
        partition: "1",
        id: "1",
        value: "foo",
      });
    });

    it.concurrent("Removes _ttl if expiry field is set falsey", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      await cacheItemsCollection.insert({
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });

      await cacheItemsCollection.update(
        {
          id: "1",
        },
        {
          expiresAt: null,
        }
      );

      const rawItem = await cacheItemsCollection.retrieveOne({
        where: {
          id: "1",
        },
        returnRaw: true,
      });
      expect(rawItem).toEqual({
        _PK: expect.any(String),
        _SK: expect.any(String),
        _c: expect.any(String),
        _wc: 2,
        partition: "1",
        id: "1",
        value: "foo",
        expiresAt: null,
      });
    });

    it("Throws if trying to update expiry field with a dynamic update", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const cacheItemsCollection = table.createCollection<CacheItem>({
        name: `cacheItems`,
        partitionKey: "id",
        extensions: [
          new DFTtlExt({
            expiresAtField: "expiresAt",
          }),
        ],
      });

      expect(() =>
        cacheItemsCollection.updateTransaction(
          {
            id: "1",
          },
          {
            expiresAt: { $inc: 60 }, // bump their time by a minute
          }
        )
      ).toThrowError("TTL field 'expiresAt' cannot accept dynamic updates");
    });
  });

  describe("Retrieve", () => {
    async function populateCacheItemsCollection(
      cacheItemsCollection: DFCollection<CacheItem>
    ) {
      await cacheItemsCollection.insert({
        partition: "1",
        id: "expiring-later",
        value: "foo",
        expiresAt: isoStringIn7Days,
      });

      await cacheItemsCollection.insert({
        partition: "1",
        id: "no-expiry",
        value: "bar",
        expiresAt: null,
      });

      await cacheItemsCollection.retrieveMany({
        where: {
          partition: "1",
          id: "no-expiry",
        },
      });

      // manually insert an expired item
      // DfTtlExt won't allow inserting an item thats already expired
      // but to avoid using timers in test, lets pretend it was inserted a while ago
      const expiredItem: CacheItem = {
        partition: "1",
        id: "expired",
        value: "bar",
        expiresAt: isoStringYesterday,
      };

      const [pk, sk] = generateIndexStrings(
        cacheItemsCollection.config.name,
        cacheItemsCollection.config.partitionKey,
        cacheItemsCollection.config.sortKey,
        expiredItem as any
      );

      await cacheItemsCollection.table.client.put({
        TableName: cacheItemsCollection.table.tableName,
        Item: {
          _PK: pk,
          _SK: sk,
          _wc: 1,
          _c: cacheItemsCollection.config.name,
          _ttl: unixTimestampYesterday,
          ...expiredItem,
        },
      });
    }

    it.concurrent(
      "Filters out expired records if filterExpired is true and expiry field is set",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const cacheItemsCollection = table.createCollection<CacheItem>({
          name: `cacheItems`,
          partitionKey: "partition",
          sortKey: "id",
          extensions: [
            new DFTtlExt({
              expiresAtField: "expiresAt",
              filterExpired: true,
            }),
          ],
        });

        // insert test data
        await populateCacheItemsCollection(cacheItemsCollection);

        const items = await cacheItemsCollection.retrieveMany({
          where: {
            partition: "1",
          },
        });

        expect(items).toEqual([
          {
            partition: "1",
            id: "expiring-later",
            value: "foo",
            expiresAt: isoStringIn7Days,
          },
          {
            partition: "1",
            id: "no-expiry",
            value: "bar",
            expiresAt: null,
          },
        ]);
      }
    );

    it.concurrent(
      "Does not filter out expired records if filterExpired is false and expiry field is set",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const cacheItemsCollection = table.createCollection<CacheItem>({
          name: `cacheItems`,
          partitionKey: "partition",
          sortKey: "id",
          extensions: [
            new DFTtlExt({
              expiresAtField: "expiresAt",
              filterExpired: false,
            }),
          ],
        });

        // insert test data
        await populateCacheItemsCollection(cacheItemsCollection);

        const items = await cacheItemsCollection.retrieveMany({
          where: {
            partition: "1",
          },
        });

        expect(items).toEqual([
          {
            partition: "1",
            id: "expired",
            value: "bar",
            expiresAt: isoStringYesterday,
          },
          {
            partition: "1",
            id: "expiring-later",
            value: "foo",
            expiresAt: isoStringIn7Days,
          },
          {
            partition: "1",
            id: "no-expiry",
            value: "bar",
            expiresAt: null,
          },
        ]);
      }
    );
  });
});
