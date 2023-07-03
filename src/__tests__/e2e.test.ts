import { DFTable } from "../DFTable.js";
import { DFLastUpdatedExt } from "../extensions/DFLastUpdatedExt.js";
import { DFSecondaryIndexExt } from "../extensions/DFSecondaryIndexExt.js";

import { testDbConfig } from "../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../testHelpers/genTestPrefix.js";

describe("E2E tests", () => {
  it.concurrent("Users & projects flow", async () => {
    interface User {
      id: number;
      firstName: string;
      lastName: string;
      email: string;
      numProjects: number;
      lastUpdated?: string;
    }

    interface Project {
      id: number;
      ownerUserId: number;
      projectName: string;
      lastUpdated?: string;
    }

    const table = new DFTable(testDbConfig);

    const userCollection = table.createCollection<User>({
      name: `${genTestPrefix()}-user`,
      partitionKey: "id",
      extensions: [
        new DFLastUpdatedExt("lastUpdated"),
        new DFSecondaryIndexExt({
          dynamoIndex: "GSI1",
          indexName: "name",
          partitionKey: "lastName",
          sortKey: "firstName",
        }),
        new DFSecondaryIndexExt({
          dynamoIndex: "GSI2",
          indexName: "email",
          partitionKey: "email",
        }),
        // many more coming:
        //     * Unique constraint
        //     * Computed values
        //     * TTL
        //     * Local re-sort index
        //     * Denormalised count
        //     * Denormalise items (attach to another record in the table)
        //     * Join (store items in another collections partition) so multiple entities can be fetched with a single query
        //     * ZodValidation
        //     * ModelVersion upgrades / data migrations
        //     * Auto generated slug IDs (can check existence while writing & optimistic lock)
        //     * Audit history
        //     * Auto batching of reads/writes
      ],
    });

    const projectsCollection = table.createCollection<Project>({
      name: `${genTestPrefix()}-project`,
      partitionKey: "ownerUserId",
      sortKey: "id",
      extensions: [],
    });

    // insert a user
    await userCollection.insert({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      email: "jye.lewis@propelleraero.com.au",
      numProjects: 0,
    });

    // get user by ID
    await expect(
      userCollection.retrieveOne({
        where: {
          id: 1,
        },
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      email: "jye.lewis@propelleraero.com.au",
      numProjects: 0,
      lastUpdated: expect.any(String),
    });

    // get user by email
    await expect(
      userCollection.retrieveOne({
        index: "email",
        where: {
          email: "jye.lewis@propelleraero.com.au",
        },
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      email: "jye.lewis@propelleraero.com.au",
      numProjects: 0,
      lastUpdated: expect.any(String),
    });

    // get user by name search
    await expect(
      userCollection.retrieveOne({
        index: "name",
        where: {
          lastName: "Lewis",
          firstName: { $beginsWith: "J" },
        },
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      email: "jye.lewis@propelleraero.com.au",
      numProjects: 0,
      lastUpdated: expect.any(String),
    });

    // insert some projects for this user
    const projectInsertTransaction = await projectsCollection.insertTransaction(
      {
        id: 1,
        ownerUserId: 1,
        projectName: "My projectttt",
      }
    );

    projectInsertTransaction.addSecondaryTransaction(
      await userCollection.updateTransaction(
        {
          id: 1,
        },
        {
          numProjects: { $inc: 1 },
        }
      )
    );

    const savedProject = await projectInsertTransaction.commit();

    // validate our saved items
    expect(savedProject).toEqual({
      id: 1,
      ownerUserId: 1,
      projectName: "My projectttt",
    });

    // check out user 'numProjects' was incremented
    await expect(
      userCollection.retrieveOne({
        where: {
          id: 1,
        },
        consistentRead: true,
      })
    ).resolves.toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      email: "jye.lewis@propelleraero.com.au",
      numProjects: 1,
      lastUpdated: expect.any(String),
    });
  });

  it.concurrent("Projects, sites & users flow", async () => {
    interface Portal {
      id: string;
      name: string;
      dateCreated: string;
      country: string;
      state: string;
      postcode: number;
    }
    interface User {
      id: string;
      name: string;
      portalId: string;
    }
    interface Site {
      id: string;
      portalId: string;
      name: string;
      createdByUserId: string;
    }

    interface Survey {
      id: string;
      siteId: string;
      name: string;
      dateCaptured: string;
      isReleased: boolean;
    }

    const table = new DFTable(testDbConfig);

    // set up our collections (effectively tables) --------------------------------------------------------
    const portalsCollection = table.createCollection<Portal>({
      name: `${genTestPrefix()}-portal`,
      partitionKey: "id",
      extensions: [
        new DFSecondaryIndexExt({
          indexName: "byLocation",
          dynamoIndex: "GSI1",
          partitionKey: "country",
          sortKey: ["state", "postcode"],
        }),
      ],
    });
    const usersCollection = table.createCollection<User>({
      name: `${genTestPrefix()}-user`,
      partitionKey: "id",
      extensions: [
        new DFSecondaryIndexExt({
          indexName: "usersWithinPortal",
          dynamoIndex: "GSI1",
          partitionKey: "portalId",
          sortKey: "id",
        }),
      ],
    });
    const sitesCollection = table.createCollection<Site>({
      name: `${genTestPrefix()}-site`,
      partitionKey: "portalId",
      sortKey: "id",
      extensions: [],
    });
    const surveysCollection = table.createCollection<Survey>({
      name: `${genTestPrefix()}-survey`,
      partitionKey: "siteId",
      sortKey: "id",
      extensions: [
        new DFSecondaryIndexExt({
          indexName: "releasedSurveys",
          dynamoIndex: "GSI1",
          partitionKey: "siteId",
          sortKey: "dateCaptured",
          includeInIndex: [(survey) => survey.isReleased, ["isReleased"]],
        }),
      ],
    });

    // insert test data ------------------------------------------------------------------
    const portal1 = await portalsCollection.insert({
      id: "01H3K7",
      dateCreated: new Date().toISOString(),
      name: "Hudson construction",
      country: "Australia",
      state: "NSW",
      postcode: 2010,
    });
    const user1 = await usersCollection.insert({
      id: "NXRFB",
      portalId: portal1.id,
      name: "Monty Burns",
    });
    const site1 = await sitesCollection.insert({
      id: "DZT2W",
      portalId: portal1.id,
      createdByUserId: user1.id,
      name: "Demo: Mine site",
    });
    const survey1 = await surveysCollection.insert({
      id: "26Y4F",
      siteId: site1.id,
      dateCaptured: new Date().toISOString(),
      name: "South side",
      isReleased: true,
    });
    const survey2 = await surveysCollection.insert({
      id: "4W6K",
      siteId: site1.id,
      dateCaptured: new Date().toISOString(),
      name: "Whole site June",
      isReleased: false,
    });

    // query ------------------------------------------------------------------
    await expect(
      usersCollection.retrieveOne({
        where: {
          id: user1.id,
        },
      })
    ).resolves.toEqual(user1);

    await expect(
      sitesCollection.retrieveMany({
        where: {
          portalId: portal1.id,
        },
      })
    ).resolves.toEqual([site1]);

    await expect(
      usersCollection.retrieveMany({
        index: "usersWithinPortal",
        where: {
          portalId: portal1.id,
        },
      })
    ).resolves.toEqual([user1]);

    await expect(
      surveysCollection.retrieveOne({
        where: {
          siteId: site1.id,
          id: "4W6K",
        },
      })
    ).resolves.toEqual(survey2);

    await expect(
      surveysCollection.retrieveMany({
        index: "releasedSurveys",
        where: {
          siteId: site1.id,
          dateCaptured: { $gt: "2023" },
        },
      })
    ).resolves.toEqual([survey1]);

    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "Australia",
          state: "NSW",
        },
      })
    ).resolves.toEqual([portal1]);

    // search for items by location
    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "Australia",
          state: "NSW",
          postcode: { $betweenIncl: [2000, 2030] },
        },
      })
    ).resolves.toEqual([portal1]);

    // search for items by location, with a filter
    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "Australia",
          state: "NSW",
          postcode: { $betweenIncl: [2000, 2030] },
        },
        filter: {
          name: { $contains: "construct" },
        },
      })
    ).resolves.toEqual([portal1]);

    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "Australia",
          state: "NSW",
          postcode: { $betweenIncl: [2000, 2030] },
        },
        filter: {
          name: { $contains: "and sons" },
        },
      })
    ).resolves.toEqual([]);

    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "USA",
          state: "TX",
          postcode: { $betweenIncl: [2000, 2030] },
        },
      })
    ).resolves.toEqual([]);

    await expect(
      portalsCollection.retrieveMany({
        index: "byLocation",
        where: {
          country: "Australia",
          state: "NSW",
          postcode: { $betweenIncl: [2500, 2530] },
        },
      })
    ).resolves.toEqual([]);
  });
});
