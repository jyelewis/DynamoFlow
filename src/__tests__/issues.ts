import { DFTable } from "../DFTable.js";
import { testDbConfigWithPrefix } from "../testHelpers/testDbConfigs.js";
import { EntityWithMetadata } from "../types/types.js";

describe("Issues", () => {
  // conceptual vulnerability:
  // collection: PK: [portalId] SK: [username, isActivated, age]
  // collection: PK: [username, isActivated, age] SK: []
  // username could be maliciously crafted to look like the activatedParam is set, when it may not be
  // i.e "JoeBot#true#" could be a username, and the isActivated param would appear to be true
  // if the # signs weren't properly escaped

  describe("Query search vulnerability", () => {
    interface User {
      portalId: string;
      username: string;
      isActivated: boolean;
      age: number;
    }

    it.concurrent(
      "Normal behaviour, no malicious input (activated)",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "portalId",
          sortKey: ["username", "isActivated", "age"],
        });

        await usersCollection.insert({
          portalId: "portal1",
          username: "JoeBot",
          isActivated: true,
          age: 20,
        });

        // read back our user
        const user = await usersCollection.retrieveOne({
          where: {
            portalId: "portal1",
            username: "JoeBot",
            isActivated: true,
          },
        });

        expect(user?.username).toEqual("JoeBot");
      }
    );

    // BUG: this boolean check is not working (no malicious input)
    it.concurrent.failing(
      "Normal behaviour, no malicious input (not activated)",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "portalId",
          sortKey: ["username", "isActivated", "age"],
        });

        await usersCollection.insert({
          portalId: "portal1",
          username: "JoeBot",
          isActivated: false,
          age: 20,
        });

        // read back our user
        const user = await usersCollection.retrieveOne({
          where: {
            portalId: "portal1",
            username: "JoeBot",
            isActivated: true,
          },
          returnRaw: true,
        });

        // user is not activated, should not be returned
        expect(user).toEqual(null);
      }
    );

    // VULNERABILITY: User can craft a malicious username to make it look like they are activated (via sort key)
    it.concurrent.failing(
      "PK: [portalId] SK: [username, isActivated, age]",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "portalId",
          sortKey: ["username", "isActivated", "age"],
        });

        await usersCollection.insert({
          portalId: "portal1",
          username: "JoeBot#true",
          // they are NOT activated
          // the user will try to make it look like they are activated
          isActivated: false,
          age: 20,
        });

        // read back our user
        const activatedUser = await usersCollection.retrieveOne({
          where: {
            portalId: "portal1",
            username: "JoeBot",
            isActivated: true,
          },
        });

        // user should not match! They are not truly activated
        expect(activatedUser).toEqual(null);

        const readUser = await usersCollection.retrieveOne({
          where: {
            portalId: "portal1",
            username: "JoeBot#true",
            isActivated: false,
          },
        });

        expect(readUser?.username).toEqual("JoeBot#true");
      }
    );

    it.concurrent("PK: [username, isActivated] SK: [age]", async () => {
      const table = new DFTable(testDbConfigWithPrefix());
      const usersCollection = table.createCollection<User>({
        name: "users",
        partitionKey: ["username", "isActivated"],
        sortKey: ["age"],
      });

      await usersCollection.insert({
        portalId: "portal1",
        username: "JoeBot#true",
        // they are NOT activated
        // the user will try to make it look like they are activated
        isActivated: false,
        age: 20,
      });

      // read back our user
      const activatedUser = await usersCollection.retrieveOne({
        where: {
          portalId: "portal1",
          username: "JoeBot",
          isActivated: true,
        },
      });

      // user should not match! They are not truly activated
      expect(activatedUser).toEqual(null);

      const readUser = await usersCollection.retrieveOne({
        where: {
          portalId: "portal1",
          username: "JoeBot#true",
          isActivated: false,
        },
      });

      expect(readUser?.username).toEqual("JoeBot#true");
    });
  });

  describe("Sensible primary keys", () => {
    interface User {
      id: string;
      username: string;
      isActivated: boolean;
      age: number;
    }

    it.concurrent.failing(
      "Doesn't store double hashes on the end of sort key",
      async () => {
        const table = new DFTable(testDbConfigWithPrefix());
        const usersCollection = table.createCollection<User>({
          name: "users",
          partitionKey: "id",
        });

        await usersCollection.insert({
          id: "user-1",
          username: "JoeBot",
          isActivated: true,
          age: 45,
        });

        // read back our user
        const user = (await usersCollection.retrieveOne({
          where: {
            id: "user-1",
          },
          returnRaw: true,
        })) as unknown as EntityWithMetadata;

        expect(user._PK).toEqual(`${usersCollection.config.name}#user-1#`);
        expect(user._SK).toEqual(`${usersCollection.config.name}#`);
      }
    );
  });
});
