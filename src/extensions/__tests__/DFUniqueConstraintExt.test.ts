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

describe("DFUniqueConstraintExt", () => {
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

    it.concurrent("Supports unique number enforcement", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueAgeExt = new DFUniqueConstraintExt<User & { age: number }>(
        "age"
      );
      const usersCollection = table.createCollection<User & { age: number }>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueAgeExt],
      });

      await usersCollection.insert({
        ...user1,
        age: 1,
      });

      await expect(
        usersCollection.insert({
          ...user2,
          age: 1,
        })
      ).rejects.toThrow(DFUniqueConstraintConflictError);
    });

    it("Throws if setting a non string or number value", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      expect(() =>
        usersCollection.insertTransaction({
          ...user1,
          email: false,
        } as any)
      ).toThrow(
        "Field 'email' can only be a string or number due to DFUniqueConstraintExt"
      );
    });
  });

  describe("onUpdate", () => {
    it.concurrent.each([
      ["null -> null", null, null],
      ["null -> value", null, user1.email],
      ["value -> null", user1.email, null],
      ["value -> value (changed)", user1.email, user2.email],
      ["value -> value (same)", user1.email, user1.email],
    ])(
      "Can update an entity with a unique value (%s)",
      async (_, oldEmail, newEmail) => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        await usersCollection.insert({
          ...user1,
          email: oldEmail,
        });

        await usersCollection.update(
          { id: user1.id },
          {
            email: newEmail,
          }
        );

        // check correct items are in the index
        if (oldEmail !== null && oldEmail !== newEmail) {
          await expect(uniqueEmailExt.valueExists(oldEmail)).resolves.toEqual(
            false
          );
        }
        if (newEmail !== null) {
          await expect(uniqueEmailExt.valueExists(newEmail)).resolves.toEqual(
            true
          );
        }
      }
    );

    it.concurrent(
      "Handles multiple updates to the same entity with a unique value",
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
          email: "1@gmail.com",
        });

        // this isn't a deterministic test, these updates may happen sequentially by chance
        // but, this is also the typical case where this could happen, so nice to have a test
        // we have a test below that will consistently interrupt the updates
        await Promise.all([
          usersCollection.update(
            {
              id: user1.id,
            },
            {
              firstName: "James",
            }
          ),
          usersCollection.update(
            {
              id: user1.id,
            },
            {
              email: "2@gmail.com",
            }
          ),
          usersCollection.update(
            {
              id: user1.id,
            },
            {
              lastName: "G",
            }
          ),
        ]);
      }
    );

    it.concurrent("Handles interruptions to updates", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      await usersCollection.insert({
        ...user1,
        email: "1@gmail.com",
      });

      const transaction = usersCollection.updateTransaction(
        {
          id: user1.id,
        },
        {
          email: "2@gmail.com",
        }
      );

      // trick here, use a pre-commit handler to update the item right before this update commits
      // should trigger a re-try and succeed ths second time
      let numPreCommitRuns = 0;
      transaction.addPreCommitHandler(async () => {
        numPreCommitRuns++;

        // only interrupt on the first run
        if (numPreCommitRuns === 1) {
          await usersCollection.update(
            {
              id: user1.id,
            },
            {
              firstName: "James",
            }
          );
        }
      });

      await expect(uniqueEmailExt.valueExists("1@gmail.com")).resolves.toEqual(
        true
      );
      await expect(uniqueEmailExt.valueExists("2@gmail.com")).resolves.toEqual(
        false
      );

      await transaction.commit();

      // should have had to retry once
      expect(numPreCommitRuns).toEqual(2);

      await expect(uniqueEmailExt.valueExists("1@gmail.com")).resolves.toEqual(
        false
      );
      await expect(uniqueEmailExt.valueExists("2@gmail.com")).resolves.toEqual(
        true
      );
    });

    it.concurrent(
      "Throws if updating to a unique field value that is already in use",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        await Promise.all([
          usersCollection.insert(user1),
          usersCollection.insert(user2),
        ]);

        await expect(
          usersCollection.update(
            {
              id: user1.id,
            },
            {
              email: user2.email,
            }
          )
        ).rejects.toThrow("Unique constraint violation on field 'email'");
      }
    );

    it.concurrent("Handles item being deleted mid-update", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      await usersCollection.insert({
        ...user1,
        email: "1@gmail.com",
      });

      const transaction = usersCollection.updateTransaction(
        {
          id: user1.id,
        },
        {
          email: "2@gmail.com",
        }
      );

      // trick here, use a pre-commit handler to delete the item right before this update commits
      let numPreCommitRuns = 0;
      transaction.addPreCommitHandler(async () => {
        numPreCommitRuns++;

        // only interrupt on the first run
        if (numPreCommitRuns === 1) {
          await usersCollection.delete({
            id: user1.id,
          });
        }
      });

      await expect(uniqueEmailExt.valueExists("1@gmail.com")).resolves.toEqual(
        true
      );
      await expect(uniqueEmailExt.valueExists("2@gmail.com")).resolves.toEqual(
        false
      );

      await expect(transaction.commit()).rejects.toThrow(
        "Item was deleted while being updated"
      );

      // should have had to retry once
      expect(numPreCommitRuns).toEqual(2);

      // both emails should be available, because our item was deleted
      await expect(uniqueEmailExt.valueExists("1@gmail.com")).resolves.toEqual(
        false
      );
      await expect(uniqueEmailExt.valueExists("2@gmail.com")).resolves.toEqual(
        false
      );
    });

    it("Throws if updating with a non literal value", () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      expect(() =>
        usersCollection.updateTransaction(
          {
            id: user1.id,
          },
          {
            email: { $setIfNotExists: "2@gmail.com" },
          }
        )
      ).toThrow(
        "Field 'email' cannot be updated with dynamic values due to DFUniqueConstraintExt"
      );
    });

    it("Throws if updating with a non string or number value", () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [uniqueEmailExt],
      });

      expect(() =>
        usersCollection.updateTransaction(
          {
            id: user1.id,
          },
          {
            email: false as any,
          }
        )
      ).toThrow(
        "Field 'email' can only be a string, number or null due to DFUniqueConstraintExt"
      );
    });
  });

  describe("onDelete", () => {
    it.concurrent("Can delete item with unique value", async () => {
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

      await usersCollection.delete({ id: user1.id });

      await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
        false
      );
    });

    it.concurrent("Can delete item with null unique value", async () => {
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

      await usersCollection.insert({
        ...user1,
        email: null,
      });

      await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
        false
      );

      await usersCollection.delete({ id: user1.id });

      await expect(uniqueEmailExt.valueExists(user1.email!)).resolves.toEqual(
        false
      );
    });

    it.concurrent("Handles already deleted items", async () => {
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

      expect(await uniqueEmailExt.valueExists(user1.email!)).toEqual(false);
      await usersCollection.insert(user1);

      expect(await uniqueEmailExt.valueExists(user1.email!)).toEqual(true);

      await usersCollection.delete({ id: user1.id });

      expect(await uniqueEmailExt.valueExists(user1.email!)).toEqual(false);

      // delete again (someone else has already deleted this item)
      await usersCollection.delete({ id: user1.id });

      expect(await uniqueEmailExt.valueExists(user1.email!)).toEqual(false);
    });

    it.concurrent("Handles interrupted deletes", async () => {
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

      const transaction = usersCollection.deleteTransaction({ id: user1.id });

      // use a pre-commit handler to interrupt this transaction and delete the item
      transaction.addPreCommitHandler(async () => {
        await usersCollection.delete({ id: user1.id });
      });

      // shouldn't crash
      await transaction.commit();

      // item should be removed from index
      expect(await uniqueEmailExt.valueExists(user1.email!)).toEqual(false);
    });
  });

  describe("migrateEntity", () => {
    it.each([
      [undefined, 0],
      [null, 0],
      [user1.email, 1],
    ])(
      "Only creates unique index item if the value is defined (%p)",
      (email, expectedSecondaryTransactions) => {
        const table = new DFTable(testDbConfigWithPrefix());
        const uniqueEmailExt = new DFUniqueConstraintExt<User>("email");
        table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [uniqueEmailExt],
        });

        const migrationTransaction = table.createTransaction({
          type: "Update",
          key: {
            _PK: "my-item",
            _SK: "my-item",
          },
          updateValues: {},
        });

        uniqueEmailExt.migrateEntity(
          {
            ...user1,
            email: email as any,
          },
          migrationTransaction
        );

        expect(migrationTransaction.secondaryOperations.length).toEqual(
          expectedSecondaryTransactions
        );
      }
    );
  });
});
