import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import {
  DFUniqueConstraintConflictError,
  DFUniqueConstraintExt,
} from "../DFUniqueConstraintExt.js";

interface User {
  id: string;
  firstName: string;
  lastName: string;
  email: null | string;
}

const user1: User = {
  id: "user-1",
  firstName: "John",
  lastName: "Smith",
  email: "john.smith@gmail.com",
};

const user2: User = {
  id: "user-2",
  firstName: "Joe",
  lastName: "Blogs",
  email: "joe.blogs@hotmail.com",
};

describe("DFUniqueConstraintEtc", () => {
  describe("onInsert", () => {
    it.concurrent("Can insert a new entity with a unique value", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
        false
      );

      await usersCollection.insert(user1);

      await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
        true
      );
    });

    it.concurrent("Can insert a new entity null unique value", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      await usersCollection.insert({
        ...user1,
        email: null,
      });
    });

    it.concurrent(
      "Can insert multiple entities null unique value",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        await usersCollection.insert({
          ...user1,
          email: null,
        });

        await usersCollection.insert({
          ...user2,
          email: null,
        });
      }
    );

    it.concurrent(
      "Can insert multiple entities with unique values",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        await usersCollection.insert(user1);

        await usersCollection.insert(user2);

        await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
          true
        );
        await expect(uniqueEmailExt.valueExists(user2.email!)).resolves.toEqual(
          true
        );
      }
    );

    it.concurrent(
      "Throws when inserting a new entity with a duplicate unique value",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        await usersCollection.insert(user1);

        await expect(
          usersCollection.insert({
            ...user2,
            email: user1.email,
          })
        ).rejects.toThrow(DFUniqueConstraintConflictError);
      }
    );
  });

  describe("onUpdate", () => {
    it.todo("Can update an entity with a unique value (null -> null)");
    it.todo("Can update an entity with a unique value (null -> value)");
    it.todo("Can update an entity with a unique value (value -> value)");
    it.todo("Can update an entity with a unique value (value -> null)");
    it.todo("Handles multiple updates to the same entity with a unique value");
  });

  // describe("onDelete");

  // describe("migrateEntity");
});
