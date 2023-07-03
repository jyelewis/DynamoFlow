import { DFTable } from "../../DFTable.js";
import { testDbConfig } from "../../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../../testHelpers/genTestPrefix.js";
import { DFSecondaryIndexExt } from "../DFSecondaryIndexExt.js";

interface User {
  id: number;
  firstName: string;
  lastName: string;
  userName: null | string;
  isActivated: boolean;
  age: null | number;
  lastUpdated: null | string;
}

describe("DFSecondaryIndexExt", () => {
  it("Throws if the DB has no GSIs", () => {
    const table = new DFTable({
      ...testDbConfig,
      GSIs: undefined,
    });

    expect(() => {
      table.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "name",
            dynamoIndex: "GSI1",
            partitionKey: "lastName",
            sortKey: "firstName",
          }),
        ],
      });
    }).toThrowError("DB does not have any GSIs defined");
  });

  it("Throws if the DB does not have the required GSI defined", () => {
    const table = new DFTable({
      ...testDbConfig,
      GSIs: ["GSI1", "GSI2"],
    });

    expect(() => {
      table.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "name",
            dynamoIndex: "GSI3",
            partitionKey: "lastName",
            sortKey: "firstName",
          }),
        ],
      });
    }).toThrowError("GSI 'GSI3' not defined for this DB");
  });

  it.concurrent("Writes & retrieves from secondary index", async () => {
    const table = new DFTable(testDbConfig);

    const usersCollection = table.createCollection<User>({
      name: `${genTestPrefix()}-user`,
      partitionKey: "id",
      extensions: [
        new DFSecondaryIndexExt({
          indexName: "name",
          dynamoIndex: "GSI1",
          partitionKey: "lastName",
          sortKey: "firstName",
        }),
      ],
    });

    const user1: User = {
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      userName: null,
      age: null,
      lastUpdated: null,
    };

    const user2: User = {
      id: 2,
      firstName: "Joe",
      lastName: "Lewis",
      isActivated: true,
      userName: null,
      age: null,
      lastUpdated: null,
    };

    await usersCollection.insert(user1);
    await usersCollection.insert(user2);

    // try querying our index
    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
        },
      })
    ).resolves.toEqual([user2, user1]);

    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: "Jye",
        },
      })
    ).resolves.toEqual([user1]);

    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: "Joe",
        },
      })
    ).resolves.toEqual([user2]);

    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: { $beginsWith: "Jo" },
        },
      })
    ).resolves.toEqual([user2]);

    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: "Jo",
        },
      })
    ).resolves.toEqual([]);

    await expect(
      usersCollection.retrieveMany({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: { $beginsWith: "J" },
        },
      })
    ).resolves.toEqual([user2, user1]);
  });

  it.concurrent(
    "Writes & retrieves from secondary index with a composite key",
    async () => {
      const table = new DFTable(testDbConfig);

      const usersCollection = table.createCollection<User>({
        name: `${genTestPrefix()}-user`,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "over21",
            dynamoIndex: "GSI1",
            includeInIndex: [
              (user: User) => !!(user.age && user.age >= 21),
              ["age"],
            ],
            partitionKey: ["userName", "isActivated"],
            sortKey: ["lastName", "firstName"],
          }),
        ],
      });

      const user1: User = {
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        userName: "jyelewis",
        age: 29,
        lastUpdated: null,
      };
      const user2: User = {
        id: 2,
        firstName: "Joe",
        lastName: "Lewis",
        isActivated: true,
        userName: "joe_bot",
        age: 15,
        lastUpdated: null,
      };

      await usersCollection.insert(user1);
      await usersCollection.insert(user2);

      // try querying our index
      await expect(
        usersCollection.retrieveMany({
          index: "over21",
          where: {
            isActivated: false,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([]);

      await expect(
        usersCollection.retrieveMany({
          index: "over21",
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([user1]);

      await expect(
        usersCollection.retrieveMany({
          index: "over21",
          where: {
            isActivated: true,
            userName: "joe_bot",
          },
        })
      ).resolves.toEqual([]);

      await usersCollection.update(
        {
          id: 2,
        },
        {
          // mark 21, should now be included in the GSI index
          age: 21,
          // provide all other fields to avoid doing a pre-fetch
          isActivated: true,
          userName: "joe_bot",
          firstName: "Joe",
          lastName: "Lewis",
        }
      );

      await expect(
        usersCollection.retrieveMany({
          index: "over21",
          where: {
            isActivated: true,
            userName: "joe_bot",
          },
        })
      ).resolves.toEqual([
        {
          ...user2,
          age: 21,
        },
      ]);

      await usersCollection.update(
        {
          id: 2,
        },
        {
          // mark 15 again, should be removed from the index
          age: 15,
        }
      );

      await expect(
        usersCollection.retrieveMany({
          index: "over21",
          where: {
            isActivated: true,
            userName: "joe_bot",
          },
        })
      ).resolves.toEqual([]);
    }
  );

  it.concurrent(
    "Migrates entities to new index schemas on read if needed",
    async () => {
      const table = new DFTable(testDbConfig);

      const userCollectionName = `${genTestPrefix()}-user`;
      const usersCollectionPreIndex = table.createCollection<User>({
        name: userCollectionName,
        partitionKey: "id",
        extensions: [],
      });

      const usersCollection = table.createCollection<User>({
        ...usersCollectionPreIndex.config,
        name: userCollectionName + "-2",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "over21",
            dynamoIndex: "GSI1",
            includeInIndex: [
              (user: User) => !!(user.age && user.age >= 21),
              ["age"],
            ],
            partitionKey: ["userName", "isActivated"],
            sortKey: ["lastName", "firstName"],
          }),
        ],
      });
      // trick to get around the error for registering 2 collections with the same name
      usersCollection.config.name = userCollectionName;

      const user1: User = {
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        userName: "jyelewis",
        age: 29,
        lastUpdated: null,
      };
      const user2: User = {
        id: 2,
        firstName: "Joe",
        lastName: "Lewis",
        isActivated: true,
        userName: "joe_bot",
        age: 15,
        lastUpdated: null,
      };

      await usersCollectionPreIndex.insert(user1);
      await usersCollectionPreIndex.insert(user2);

      // try querying using the collection with this GSI registered
      await expect(
        usersCollection.retrieveOne({
          where: {
            id: user1.id,
          },
          returnRaw: true,
        })
      ).resolves.toEqual({
        ...user1,
        _PK: `${userCollectionName}#0000000001.000000#`,
        _SK: `${userCollectionName}##`,
        _c: userCollectionName,
        _wc: 1,
        age: 29,
        firstName: "Jye",
        id: 1,
        isActivated: true,
        lastName: "Lewis",
        lastUpdated: null,
        userName: "jyelewis",
      });
    }
  );

  // TODO: finish writing tests here
});
