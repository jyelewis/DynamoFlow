import { DFDB } from "../../DFDB.js";
import { testDbConfig } from "../../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../../testHelpers/genTestPrefix.js";
import { DFSecondaryIndexExt } from "../DFSecondaryIndexExt.js";

interface User {
  id: number;
  firstName: string;
  lastName: string;
  isActivated: boolean;
  age?: number;
  lastUpdated?: string;
}

describe("DFSecondaryIndexExt", () => {
  it.concurrent("Writes & retrieves from secondary index", async () => {
    const db = new DFDB(testDbConfig);

    const usersCollection = db.createCollection<User>({
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
    };

    const user2: User = {
      id: 2,
      firstName: "Joe",
      lastName: "Lewis",
      isActivated: true,
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
});
