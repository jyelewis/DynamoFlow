import { testDbConfig } from "../../testHelpers/testDbConfigs.js";
import { DFTable } from "../../DFTable.js";
import { genTestPrefix } from "../../testHelpers/genTestPrefix.js";
import { DFMigrationExt } from "../DFMigrationExt.js";

describe("DFMigrationExt", () => {
  const collectionName = `${genTestPrefix()}-users`;

  it("Creates & populates a table of entities without extension enabled", async () => {
    const table = new DFTable(testDbConfig);

    interface User {
      id: number;
      firstName: string;
      lastName: string;
    }

    const usersCollection = table.createCollection<User>({
      name: collectionName, // re-use the same collection name as if this application is restarting with new code each test
      partitionKey: "id",
    });

    await Promise.all([
      usersCollection.insert({ id: 1, firstName: "Walter", lastName: "White" }),
      usersCollection.insert({
        id: 2,
        firstName: "Jesse",
        lastName: "Pinkman",
      }),
      usersCollection.insert({ id: 3, firstName: "Saul", lastName: "Goodman" }),
      usersCollection.insert({ id: 4, firstName: "Gus", lastName: "Fring" }),
    ]);

    // can get items as expected
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual({ id: 1, firstName: "Walter", lastName: "White" });
  });

  it("Can introduce new migration extension", async () => {
    const table = new DFTable(testDbConfig);

    interface User {
      id: number;
      firstName: string;
      lastName: string;
      fullName: string;
    }

    let migrationCount = 0;

    const usersCollection = table.createCollection<User>({
      name: collectionName, // re-use the same collection name as if this application is restarting with new code each test
      partitionKey: "id",
      extensions: [
        new DFMigrationExt<User>({
          version: 1,
          migrateEntity: (currentEntityVersion, entity) => {
            migrationCount++;
            return {
              fullName: `${entity.firstName} ${entity.lastName}`,
            };
          },
        }),
      ],
    });

    expect(migrationCount).toEqual(0);

    // can get items as expected
    // with migration on demand
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Walter",
      lastName: "White",
      fullName: "Walter White",
    });

    expect(migrationCount).toEqual(1);

    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 2,
        },
      })
    ).resolves.toEqual({
      id: 2,
      firstName: "Jesse",
      lastName: "Pinkman",
      fullName: "Jesse Pinkman",
    });

    expect(migrationCount).toEqual(2);
  });

  it("Stores new entities with the appropriate version information", async () => {
    const table = new DFTable(testDbConfig);

    interface User {
      id: number;
      firstName: string;
      lastName: string;
      fullName: string;
    }

    let migrationCount = 0;

    const usersCollection = table.createCollection<User>({
      name: collectionName, // re-use the same collection name as if this application is restarting with new code each test
      partitionKey: "id",
      extensions: [
        new DFMigrationExt<User>({
          version: 1,
          migrateEntity: (currentEntityVersion, entity) => {
            migrationCount++;
            return {
              fullName: `${entity.firstName} ${entity.lastName}`,
            };
          },
        }),
      ],
    });

    expect(migrationCount).toEqual(0);

    await usersCollection.insert({
      id: 5,
      firstName: "Joe",
      lastName: "Bot",
      fullName: "Joe Bot",
    });

    expect(migrationCount).toEqual(0);

    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 5,
        },
      })
    ).resolves.toEqual({
      id: 5,
      firstName: "Joe",
      lastName: "Bot",
      fullName: "Joe Bot",
    });

    expect(migrationCount).toEqual(0);
  });

  it("Re-migrates if the version number changes", async () => {
    const table = new DFTable(testDbConfig);

    interface User {
      id: number;
      firstName: string;
      lastName: string;
      fullName: string;
      isLegacyUser: boolean;
    }

    let migrationCount = 0;

    const usersCollection = table.createCollection<User>({
      name: collectionName, // re-use the same collection name as if this application is restarting with new code each test
      partitionKey: "id",
      extensions: [
        new DFMigrationExt<User>({
          version: 2,
          migrateEntity: (currentEntityVersion, entity) => {
            migrationCount++;
            return {
              fullName: `${entity.firstName} ${entity.lastName}`,
              isLegacyUser: true,
            };
          },
        }),
      ],
    });

    expect(migrationCount).toEqual(0);

    // can get items as expected
    // with migration on demand
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Walter",
      lastName: "White",
      fullName: "Walter White",
      isLegacyUser: true,
    });

    expect(migrationCount).toEqual(1);

    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 3,
        },
      })
    ).resolves.toEqual({
      id: 3,
      firstName: "Saul",
      lastName: "Goodman",
      fullName: "Saul Goodman",
      isLegacyUser: true,
    });

    expect(migrationCount).toEqual(2);

    // entity we created last test with version number 1
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: 5,
        },
      })
    ).resolves.toEqual({
      id: 5,
      firstName: "Joe",
      lastName: "Bot",
      fullName: "Joe Bot",
      isLegacyUser: true,
    });

    expect(migrationCount).toEqual(3);
  });
});
