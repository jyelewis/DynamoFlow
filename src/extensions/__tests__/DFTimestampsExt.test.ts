import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import { DFTimestampsExt } from "../DFTimestampsExt.js";

interface User {
  id: number;
  firstName: string;
  lastName: string;
  isActivated: boolean;
  age?: number;
  dateCreated?: string;
  lastUpdated?: string;
}

const user1: User = {
  id: 1,
  firstName: "Jye",
  lastName: "Lewis",
  isActivated: true,
  dateCreated: "original value",
  lastUpdated: "original value",
};
describe("DFTimestamps", () => {
  it.concurrent("Populates fields on insert (none set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [new DFTimestampsExt({})],
    });

    const insertedUser = await usersCollection.insert(user1);

    expect(insertedUser).toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      dateCreated: "original value",
      lastUpdated: "original value",
    });
  });

  it.concurrent("Populates fields on insert (createdAt set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          createdAtField: "dateCreated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    expect(insertedUser).toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      dateCreated: expect.not.stringMatching("original value"),
      lastUpdated: "original value",
    });
  });

  it.concurrent("Populates fields on insert (updatedAt set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          updatedAtField: "lastUpdated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    expect(insertedUser).toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      dateCreated: "original value",
      lastUpdated: expect.not.stringMatching("original value"),
    });
  });

  it.concurrent(
    "Populates fields on insert (createdAt & updatedAt set)",
    async () => {
      const table = new DFTable(testDbConfigWithPrefix());

      const usersCollection = table.createCollection<User>({
        name: `user`,
        partitionKey: "id",
        extensions: [
          new DFTimestampsExt({
            createdAtField: "dateCreated",
            updatedAtField: "lastUpdated",
          }),
        ],
      });

      const insertedUser = await usersCollection.insert(user1);

      expect(insertedUser).toEqual({
        id: 1,
        firstName: "Jye",
        lastName: "Lewis",
        isActivated: true,
        dateCreated: expect.not.stringMatching("original value"),
        lastUpdated: expect.not.stringMatching("original value"),
      });
    }
  );

  it.concurrent("Updates (none set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [new DFTimestampsExt({})],
    });

    await usersCollection.insert(user1);

    const updatedUser = await usersCollection.update(
      {
        id: user1.id,
      },
      {
        firstName: "Joe",
      }
    );

    expect(updatedUser.dateCreated).toEqual("original value");
    expect(updatedUser.lastUpdated).toEqual("original value");
  });

  it.concurrent("Updates (createdAt set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          createdAtField: "dateCreated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    const updatedUser = await usersCollection.update(
      {
        id: user1.id,
      },
      {
        firstName: "Joe",
      }
    );

    expect(updatedUser.dateCreated).toEqual(insertedUser.dateCreated);
    expect(updatedUser.lastUpdated).toEqual("original value");
  });

  it.concurrent("Updates (updatedAt set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          updatedAtField: "lastUpdated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    const updatedUser = await usersCollection.update(
      {
        id: user1.id,
      },
      {
        firstName: "Joe",
      }
    );

    expect(updatedUser.dateCreated).toEqual("original value");
    expect(updatedUser.lastUpdated).not.toEqual(insertedUser.lastUpdated);
  });

  it.concurrent("Updates (createdAt & updatedAt set)", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          createdAtField: "dateCreated",
          updatedAtField: "lastUpdated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    const updatedUser = await usersCollection.update(
      {
        id: user1.id,
      },
      {
        firstName: "Joe",
      }
    );

    expect(updatedUser.dateCreated).toEqual(insertedUser.dateCreated);
    expect(updatedUser.lastUpdated).not.toEqual(insertedUser.lastUpdated);
  });

  it.concurrent("Prevents overwriting createdAt", async () => {
    const table = new DFTable(testDbConfigWithPrefix());

    const usersCollection = table.createCollection<User>({
      name: `user`,
      partitionKey: "id",
      extensions: [
        new DFTimestampsExt({
          createdAtField: "dateCreated",
          updatedAtField: "lastUpdated",
        }),
      ],
    });

    const insertedUser = await usersCollection.insert(user1);

    const updatedUser = await usersCollection.update(
      {
        id: user1.id,
      },
      {
        firstName: "Joe",
        dateCreated: "new value",
      }
    );

    expect(updatedUser.dateCreated).toEqual(insertedUser.dateCreated);
    expect(updatedUser.lastUpdated).not.toEqual(insertedUser.lastUpdated);
  });
});
