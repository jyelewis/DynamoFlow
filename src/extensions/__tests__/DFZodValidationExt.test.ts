import { DFZodValidationExt } from "../DFZodValidationExt.js";
import z from "zod";
import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";

const userSchema = z.object({
  id: z.string(),
  firstName: z.string(),
  lastName: z.string(),
  age: z.coerce.number().nullable(),
});
type User = z.infer<typeof userSchema>;

const validUser: User = {
  id: "valid",
  firstName: "John",
  lastName: "Doe",
  age: 30,
};

const invalidUser = {
  id: "invalid",
  firstName: null, // required
  lastName: "Smith",
  age: 30, // should be a number
} as any as User; // don't let typescript save us here

const validUserNeedsConverting = {
  id: "valid-needs-converting",
  firstName: "James",
  lastName: "Dean",
  age: "30", // should be a number
} as any as User; // don't let typescript save us here

const validUserConverted: User = {
  id: "valid-needs-converting",
  firstName: "James",
  lastName: "Dean",
  age: 30,
};

describe("DFZodValidationExt", () => {
  describe("Constructor", () => {
    it("Defines schema for collection", () => {
      const ext = new DFZodValidationExt({
        schema: userSchema,
      });

      expect(ext.config.schema).toEqual(userSchema);
    });

    it("Defaults all options to on", () => {
      const ext = new DFZodValidationExt({
        schema: userSchema,
      });

      expect(ext.config.validateOnInsert).toEqual(true);
      expect(ext.config.validateOnUpdate).toEqual(true);
      expect(ext.config.validateOnRetrieve).toEqual(true);
    });

    it("Allows overriding options", () => {
      const ext = new DFZodValidationExt({
        schema: userSchema,
        validateOnUpdate: true,
        validateOnRetrieve: false,
      });

      expect(ext.config.validateOnInsert).toEqual(true);
      expect(ext.config.validateOnUpdate).toEqual(true);
      expect(ext.config.validateOnRetrieve).toEqual(false);
    });

    it("Throws if validateOnInsert is disabled but retrieve is enabled", () => {
      expect(
        () =>
          new DFZodValidationExt({
            schema: userSchema,
            validateOnInsert: false,
            validateOnRetrieve: true,
          })
      ).toThrow(
        "If validateOnRetrieve is enabled, validateOnInsert & validateOnUpdate must also be enabled"
      );
    });

    it("Throws if validateOnUpdate is disabled but retrieve is enabled", () => {
      expect(
        () =>
          new DFZodValidationExt({
            schema: userSchema,
            validateOnUpdate: false,
            validateOnRetrieve: true,
          })
      ).toThrow(
        "If validateOnRetrieve is enabled, validateOnInsert & validateOnUpdate must also be enabled"
      );
    });
  });

  describe("onInsert", () => {
    it.concurrent(
      "Ignores validation errors if validateOnInsert is false",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [
            new DFZodValidationExt({
              schema: userSchema,
              validateOnInsert: false,
              validateOnRetrieve: false,
            }),
          ],
        });

        const insertedUser = await usersCollection.insert(invalidUser);
        expect(insertedUser).toEqual(invalidUser);
      }
    );

    it.concurrent(
      "Throws if data does not conform to schema before inserting",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [
            new DFZodValidationExt({
              schema: userSchema,
              validateOnInsert: true,
            }),
          ],
        });

        await expect(usersCollection.insert(invalidUser)).rejects.toThrow(
          "Expected string, received null"
        );

        // validate the item wasn't stored
        const retrievedUser = await usersCollection.retrieveOne({
          where: {
            id: invalidUser.id,
          },
        });
        expect(retrievedUser).toEqual(null);
      }
    );

    it.concurrent("Stores valid item as-is", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [
          new DFZodValidationExt({
            schema: userSchema,
            validateOnInsert: true,
          }),
        ],
      });

      await usersCollection.insert(validUser);

      // validate the item wasn't stored
      const retrievedUser = await usersCollection.retrieveOne({
        where: {
          id: validUser.id,
        },
      });
      expect(retrievedUser).toEqual(validUser);
    });

    it.concurrent("Stores parsed version of data", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [
          new DFZodValidationExt({
            schema: userSchema,
            validateOnInsert: true,
          }),
        ],
      });

      await usersCollection.insert(validUserNeedsConverting);

      // validate the item wasn't stored
      const retrievedUser = await usersCollection.retrieveOne({
        where: {
          id: validUserNeedsConverting.id,
        },
      });
      expect(retrievedUser).toEqual(validUserConverted);
    });
  });

  describe("onUpdate", () => {
    it.concurrent(
      "Ignores validation errors if validateOnInsert is false",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [
            new DFZodValidationExt({
              schema: userSchema,
              validateOnUpdate: false,
              validateOnRetrieve: false,
            }),
          ],
        });

        await usersCollection.insert(validUser);

        await usersCollection.update(
          { id: validUser.id },
          {
            firstName: null as any,
          }
        );

        const updatedUser = await usersCollection.retrieveOne({
          where: {
            id: validUser.id,
          },
        });
        expect(updatedUser).toEqual({
          ...validUser,
          firstName: null,
        });
      }
    );

    it.concurrent(
      "Throws if data does not conform to schema before inserting",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [
            new DFZodValidationExt({
              schema: userSchema,
            }),
          ],
        });

        await usersCollection.insert(validUser);

        await expect(
          usersCollection.update(
            { id: validUser.id },
            {
              firstName: null as any,
            }
          )
        ).rejects.toThrow("Expected string, received null");

        const updatedUser = await usersCollection.retrieveOne({
          where: {
            id: validUser.id,
          },
        });
        // check our bad changes weren't applied
        expect(updatedUser).toEqual(validUser);
      }
    );

    it.concurrent("Stores parsed version of data in item", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [
          new DFZodValidationExt({
            schema: userSchema,
          }),
        ],
      });

      await usersCollection.insert(validUser);

      await usersCollection.update(
        { id: validUser.id },
        {
          // invalid, but can be coerced to correct type (number)
          age: "20" as any,
        }
      );

      const updatedUser = await usersCollection.retrieveOne({
        where: {
          id: validUser.id,
        },
      });
      // check our bad changes weren't applied
      expect(updatedUser).toEqual({
        ...validUser,
        age: 20,
      });
    });
  });

  describe("onRetrieve", () => {
    it.concurrent(
      "Ignores validation errors if validateOnInsert is false",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [
            new DFZodValidationExt({
              schema: userSchema,
              validateOnInsert: false,
              validateOnRetrieve: false,
            }),
          ],
        });

        await usersCollection.insert(invalidUser);

        // check the item was inserted & retrieved even though it's not valid
        const insertedUser = await usersCollection.retrieveOne({
          where: {
            id: invalidUser.id,
          },
        });
        expect(insertedUser).toEqual(invalidUser);
      }
    );

    it.concurrent(
      "Throws if data does not conform to schema and does not return entity",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const ext = new DFZodValidationExt<User>({
          schema: userSchema,
          validateOnInsert: false,
          validateOnRetrieve: false,
        });
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
          extensions: [ext],
        });

        await usersCollection.insert(invalidUser);

        // now that we've inserted our bad data, enable validation
        ext.config.validateOnRetrieve = true;

        // check the item was inserted & retrieved even though it's not valid
        await expect(
          usersCollection.retrieveOne({
            where: {
              id: invalidUser.id,
            },
          })
        ).rejects.toThrow("Expected string, received null");
      }
    );

    it.concurrent("Returns parsed version of item", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const ext = new DFZodValidationExt<User>({
        schema: userSchema,
        validateOnInsert: false,
        validateOnRetrieve: false,
      });
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: "id",
        extensions: [ext],
      });

      await usersCollection.insert(validUserNeedsConverting);

      ext.config.validateOnRetrieve = true;

      // check the item was inserted & retrieved even though it's not valid
      const insertedUser = await usersCollection.retrieveOne({
        where: {
          id: validUserNeedsConverting.id,
        },
      });
      expect(insertedUser).toEqual(validUserConverted);
    });
  });
});
