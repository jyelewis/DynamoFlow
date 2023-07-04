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

    // ensure we can still query the primary index!
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual(user1);

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
    "Writes & retrieves from secondary index with a composite key (all fields provided)",
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
    "Writes & retrieves from secondary index with a composite key (fields missing, requires fetch)",
    async () => {
      const table = new DFTable(testDbConfig);

      const collectionName = `${genTestPrefix()}-user`;
      const usersCollection = table.createCollection<User>({
        name: collectionName,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "isActivated",
            dynamoIndex: "GSI1",
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

      await usersCollection.insert(user1);

      // try querying our index
      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([user1]);

      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          returnRaw: true,
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([
        {
          ...user1,
          _PK: `${collectionName}#0000000001.000000#`,
          _SK: `${collectionName}##`,
          _GSI1PK: `${collectionName}#jyelewis#1#`,
          _GSI1SK: `${collectionName}#lewis#jye#`,
          _c: collectionName,
          _wc: 1,
        },
      ]);

      await usersCollection.update(
        {
          id: 1,
        },
        {
          // update last name, GSI will need to be re-created
          lastName: "Smith",
        }
      );

      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          returnRaw: true,
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([
        {
          ...user1,
          lastName: "Smith",
          _PK: `${collectionName}#0000000001.000000#`,
          _SK: `${collectionName}##`,
          _GSI1PK: `${collectionName}#jyelewis#1#`,
          _GSI1SK: `${collectionName}#smith#jye#`,
          _c: collectionName,
          _wc: 2,
        },
      ]);
    }
  );

  it.concurrent(
    "Writes & retrieves without touching the secondary index properties",
    async () => {
      const table = new DFTable(testDbConfig);

      const collectionName = `${genTestPrefix()}-user`;
      const usersCollection = table.createCollection<User>({
        name: collectionName,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "isActivated",
            dynamoIndex: "GSI1",
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

      await usersCollection.insert(user1);

      // try querying our index
      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([user1]);

      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          returnRaw: true,
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([
        {
          ...user1,
          _PK: `${collectionName}#0000000001.000000#`,
          _SK: `${collectionName}##`,
          _GSI1PK: `${collectionName}#jyelewis#1#`,
          _GSI1SK: `${collectionName}#lewis#jye#`,
          _c: collectionName,
          _wc: 1,
        },
      ]);

      await usersCollection.update(
        {
          id: 1,
        },
        {
          // update age, shouldn't require changing the GSI at all
          age: 32,
        }
      );

      await expect(
        usersCollection.retrieveMany({
          index: "isActivated",
          returnRaw: true,
          where: {
            isActivated: true,
            userName: "jyelewis",
          },
        })
      ).resolves.toEqual([
        {
          ...user1,
          age: 32,
          _PK: `${collectionName}#0000000001.000000#`,
          _SK: `${collectionName}##`,
          _GSI1PK: `${collectionName}#jyelewis#1#`,
          _GSI1SK: `${collectionName}#lewis#jye#`,
          _c: collectionName,
          _wc: 2,
        },
      ]);
    }
  );

  it.concurrent(
    "Writes & retrieves from secondary index with a composite key (fields missing, requires fetch, entity doesn't exist)",
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
      await expect(
        usersCollection.update(
          {
            id: 2,
          },
          {
            // mark 21, should now be included in the GSI index
            age: 21,
            // don't provide other fields - will need to fetch the entity to generate this key
            // also the entity doesn't exist, so this should fail on pre-fetch
          }
        ) // TODO: this should throw the same as the normal not found error?
      ).rejects.toThrow(`Could not find entity with key {"id":2}`);
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

  it.concurrent(
    "Throws if trying to use a dynamic update expression on a GSI composite key",
    async () => {
      const table = new DFTable(testDbConfig);

      const userCollectionName = `${genTestPrefix()}-user`;
      const usersCollection = table.createCollection<User>({
        name: userCollectionName,
        partitionKey: "id",
        extensions: [
          new DFSecondaryIndexExt({
            indexName: "isActivated",
            dynamoIndex: "GSI1",
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

      await usersCollection.insert(user1);

      // try querying using the collection with this GSI registered
      await expect(
        usersCollection.retrieveOne({
          index: "isActivated",
          where: {
            userName: "jyelewis",
            isActivated: true,
          },
        })
      ).resolves.toEqual(user1);

      // update with a dynamic update expression
      await expect(
        usersCollection.update(
          {
            id: 1,
          },
          {
            isActivated: { $ifNotExists: false },
          }
        )
      ).rejects.toThrow(
        "Secondary index key 'isActivated' cannot accept dynamic updates"
      );
    }
  );

  it.concurrent("Retries if optimistic lock write fails", async () => {
    const table = new DFTable(testDbConfig);

    const userCollectionName = `${genTestPrefix()}-user`;
    const usersCollection = table.createCollection<User>({
      name: userCollectionName,
      partitionKey: "id",
      extensions: [
        new DFSecondaryIndexExt({
          indexName: "over21",
          dynamoIndex: "GSI1",
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

    await usersCollection.insert(user1);

    const transaction = usersCollection.updateTransaction(
      {
        id: 1,
      },
      {
        // GSI ext will need to pull the entity to compute the new GSI SK key
        lastName: "Smith",
      }
    );

    let hasInterruptedTransaction = false;
    transaction.addPreCommitHandler(async () => {
      // modify the user immediately before the commit happens
      // this will cause the GSI update to fail & re-try

      if (!hasInterruptedTransaction) {
        await usersCollection.update(
          {
            id: 1,
          },
          {
            age: 30,
          }
        );
        hasInterruptedTransaction = true;
      }
    });

    await transaction.commit();

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
      // both changes should be applied
      lastName: "Smith",
      age: 30,
      _PK: expect.any(String),
      _SK: expect.any(String),
      _GSI1PK: expect.any(String),
      _GSI1SK: expect.any(String),
      _c: expect.any(String),
      _wc: 3, // insert, update, update (incl GSI)
    });
  });

  it.concurrent("Multiple indexes can exist in parallel", async () => {
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
        new DFSecondaryIndexExt({
          indexName: "over21",
          dynamoIndex: "GSI2",
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
      age: 52,
      lastUpdated: null,
    };

    const user2: User = {
      id: 2,
      firstName: "Joe",
      lastName: "Lewis",
      isActivated: true,
      userName: null,
      age: 9,
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

    // the other index
    await expect(
      usersCollection.retrieveMany({
        index: "over21",
        where: {
          isActivated: true,
          userName: "jyelewis",
        },
      })
    ).resolves.toEqual([user1]);

    // ensure we can still query the primary index!
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual(user1);
  });

  it.concurrent(
    "Throws if multiple indexes try to use the same dynamo index",
    async () => {
      const table = new DFTable(testDbConfig);

      expect(() =>
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
        })
      ).toThrowError("'GSI1' already used by another index on collection");
    }
  );

  describe("entityRequiresMigration", () => {
    const table = new DFTable(testDbConfig);

    const over21Index = new DFSecondaryIndexExt({
      indexName: "over21",
      dynamoIndex: "GSI1",
      includeInIndex: [(user: User) => !!(user.age && user.age >= 21), ["age"]],
      partitionKey: ["userName", "isActivated"],
      sortKey: ["lastName", "firstName"],
    });

    // collection needs to exist for the index to be inited
    table.createCollection<User>({
      name: `users`,
      partitionKey: "id",
      extensions: [over21Index],
    });

    const user1: any = {
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      userName: "jyelewis",
      age: 29,
      lastUpdated: null,
      _PK: `users#0000000001.000000#`,
      _SK: `users##`,
      _GSI1PK: `users#jyelewis#1#`,
      _GSI1SK: `users#lewis#jye#`,
      _c: "users",
      _wc: 1,
    };

    it("Returns false if entity is not in the index and doesn't need to be", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
          age: 15,
          _GSI1PK: undefined,
          _GSI1SK: undefined,
        })
      ).toEqual(false);
    });

    it("Returns true if entity is not in the index and should be", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
          age: 52,
          _GSI1PK: undefined,
          _GSI1SK: undefined,
        })
      ).toEqual(true);
    });

    it("Returns true if entity is in the index and shouldn't be", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
          age: 15,
        })
      ).toEqual(true);
    });

    it("Returns false if entity is in the index and should be", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
        })
      ).toEqual(false);
    });

    it("Returns true if GSI PK is corrupt", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
          _GSI1PK: "asdf",
        })
      ).toEqual(true);
    });

    it("Returns true if GSI SK is corrupt", () => {
      expect(
        over21Index.entityRequiresMigration({
          ...user1,
          _GSI1SK: "asdf",
        })
      ).toEqual(true);
    });
  });
});
