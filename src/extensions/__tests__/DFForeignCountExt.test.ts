import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import { DFForeignCountExt } from "../DFForeignCountExt.js";
import { DFSecondaryIndexExt } from "../DFSecondaryIndexExt.js";

interface User {
  id: string;
  name: string;
  numProjects: number;
  numActiveProjects: number;
  numInactiveProjects: number;
}

interface Project {
  id: string;
  userId: null | string;
  isActive: boolean;
  name: string;
}

const user1: User = {
  id: "user-1",
  name: "Joe Bloggs",
  numActiveProjects: 0,
  numInactiveProjects: 0,
  numProjects: 0,
};

const user2: User = {
  id: "user-2",
  name: "Jane doe",
  numActiveProjects: 0,
  numInactiveProjects: 0,
  numProjects: 0,
};

const project1: Project = {
  id: "project-1",
  userId: user1.id,
  isActive: true,
  name: "Project 1",
};

const createCollections = () => {
  const table = new DFTable(testDbConfigWithPrefix());
  const projectsCollection = table.createCollection<Project>({
    name: "projects",
    partitionKey: "id",
    extensions: [
      new DFSecondaryIndexExt({
        indexName: "byUser",
        partitionKey: "userId",
        sortKey: "id",
        dynamoIndex: "GSI1",
      }),
    ],
  });

  const usersCollection = table.createCollection<User>({
    name: "users",
    partitionKey: "id",
    extensions: [
      new DFForeignCountExt<User, Project>({
        foreignCollection: projectsCollection,
        countField: "numProjects",
        foreignEntityToLocalKey: [
          (project) =>
            project.userId !== null ? { id: project.userId } : null,
          ["userId"],
        ],
      }),
      new DFForeignCountExt<User, Project>({
        foreignCollection: projectsCollection,
        countField: "numInactiveProjects",
        foreignEntityToLocalKey: [
          (project) =>
            project.userId !== null && !project.isActive
              ? { id: project.userId }
              : null,
          ["userId", "isActive"],
        ],
      }),
      new DFForeignCountExt<User, Project>({
        foreignCollection: () => projectsCollection, // test passing a function for circular references
        countField: "numActiveProjects",
        foreignEntityToLocalKey: [
          (project) =>
            project.userId !== null && project.isActive
              ? { id: project.userId }
              : null,
          ["userId", "isActive"],
        ],
      }),
    ],
  });

  return {
    table,
    usersCollection,
    projectsCollection,
  };
};

describe("DFForeignCountExt", () => {
  it("Installs remote extension", () => {
    const { projectsCollection } = createCollections();

    // we have 3 counts against this collection
    expect(projectsCollection.extensions.length).toEqual(4); // including our secondary index
  });

  describe("onInsert", () => {
    it("Does nothing if there is no relevant foreign item", () => {
      const { projectsCollection } = createCollections();

      const transaction = projectsCollection.insertTransaction({
        ...project1,
        userId: null,
      });

      expect(transaction.secondaryOperations.length).toEqual(0);
    });

    it.concurrent("Increments foreign count", async () => {
      const { usersCollection, projectsCollection } = createCollections();
      await usersCollection.insert(user1);

      await expect(
        usersCollection.retrieveOne({
          where: {
            id: user1.id,
          },
        })
      ).resolves.toEqual({
        ...user1,
        numProjects: 0,
        numActiveProjects: 0,
        numInactiveProjects: 0,
      });

      const transaction = projectsCollection.insertTransaction(project1);

      // we should have 1 secondary operation to update user.numProjects & user.numActiveProjects
      // 2 were added, however these are collapsed into a single update because they are against the same key (user)
      expect(transaction.secondaryOperations.length).toEqual(1);

      await transaction.commit();

      await expect(
        usersCollection.retrieveOne({
          where: {
            id: user1.id,
          },
        })
      ).resolves.toEqual({
        ...user1,
        numProjects: 1,
        numActiveProjects: 1,
        numInactiveProjects: 0,
      });
    });
  });

  describe("onUpdate", () => {
    it("Does nothing if no count dependent have been updated", () => {
      const { projectsCollection } = createCollections();

      const transaction = projectsCollection.updateTransaction(
        {
          userId: project1.userId,
          id: project1.id,
        },
        {
          name: "New name",
        }
      );

      expect(transaction.secondaryOperations.length).toEqual(0);
      expect(transaction.preCommitHandlers.length).toEqual(0);
    });

    it.concurrent.each([
      // null[active] -> null[active]
      {
        from: {
          userId: null,
          isActive: true,
        },
        to: {
          userId: null,
          isActive: true,
        },
        numSecondaryOperations: 0,
        pre: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // null[inactive] -> null[inactive]
      {
        from: {
          userId: null,
          isActive: false,
        },
        to: {
          userId: null,
          isActive: false,
        },
        numSecondaryOperations: 0,
        pre: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // null -> user1[active]
      {
        from: {
          userId: null,
          isActive: true,
        },
        to: {
          userId: user1.id,
          isActive: true,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // null -> user1[inactive]
      {
        from: {
          userId: null,
          isActive: true,
        },
        to: {
          userId: user1.id,
          isActive: false,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // user1[active] -> null[active]
      {
        from: {
          userId: user1.id,
          isActive: true,
        },
        to: {
          userId: null,
          isActive: true,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // user1[active] -> user2[active]
      {
        from: {
          userId: user1.id,
          isActive: true,
        },
        to: {
          userId: user2.id,
          isActive: true,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
        },
      },
      // user1[inactive] -> user1[active]
      {
        from: {
          userId: user1.id,
          isActive: false,
        },
        to: {
          userId: user1.id,
          isActive: true,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // user1[active] -> user1[inactive]
      {
        from: {
          userId: user1.id,
          isActive: true,
        },
        to: {
          userId: user1.id,
          isActive: false,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
      },
      // user1[inactive] -> user2[active]
      {
        from: {
          userId: user1.id,
          isActive: false,
        },
        to: {
          userId: user2.id,
          isActive: true,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
          user2: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 1,
            numActiveProjects: 1,
            numInactiveProjects: 0,
          },
        },
      },
      // user2[inactive] -> user2[inactive]
      {
        from: {
          userId: user2.id,
          isActive: false,
        },
        to: {
          userId: user2.id,
          isActive: false,
        },
        numSecondaryOperations: 1,
        pre: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
        },
        post: {
          user1: {
            numProjects: 0,
            numActiveProjects: 0,
            numInactiveProjects: 0,
          },
          user2: {
            numProjects: 1,
            numActiveProjects: 0,
            numInactiveProjects: 1,
          },
        },
      },
    ])("Update permeation works correctly", async (cf) => {
      const { usersCollection, projectsCollection } = createCollections();

      await usersCollection.insert(user1);
      await usersCollection.insert(user2);

      await projectsCollection.insert({
        ...project1,
        isActive: cf.from.isActive,
        userId: cf.from.userId,
      });

      const user1Pre = await usersCollection.retrieveOne({
        where: { id: user1.id },
      });
      expect(user1Pre?.numProjects).toEqual(cf.pre.user1.numProjects);
      expect(user1Pre?.numActiveProjects).toEqual(
        cf.pre.user1.numActiveProjects
      );
      expect(user1Pre?.numInactiveProjects).toEqual(
        cf.pre.user1.numInactiveProjects
      );

      const user2Pre = await usersCollection.retrieveOne({
        where: { id: user2.id },
      });
      expect(user2Pre?.numProjects).toEqual(cf.pre.user2.numProjects);
      expect(user2Pre?.numActiveProjects).toEqual(
        cf.pre.user2.numActiveProjects
      );
      expect(user2Pre?.numInactiveProjects).toEqual(
        cf.pre.user2.numInactiveProjects
      );

      await projectsCollection.update(
        { id: project1.id },
        {
          isActive: cf.to.isActive,
          userId: cf.to.userId,
        }
      );

      const user1Post = await usersCollection.retrieveOne({
        where: { id: user1.id },
      });
      expect(user1Post?.numProjects).toEqual(cf.post.user1.numProjects);
      expect(user1Post?.numActiveProjects).toEqual(
        cf.post.user1.numActiveProjects
      );
      expect(user1Post?.numInactiveProjects).toEqual(
        cf.post.user1.numInactiveProjects
      );

      const user2Post = await usersCollection.retrieveOne({
        where: { id: user2.id },
      });
      expect(user2Post?.numProjects).toEqual(cf.post.user2.numProjects);
      expect(user2Post?.numActiveProjects).toEqual(
        cf.post.user2.numActiveProjects
      );
      expect(user2Post?.numInactiveProjects).toEqual(
        cf.post.user2.numInactiveProjects
      );
    });
  });

  describe("onDelete", () => {
    it.concurrent("Does nothing if the item does not exist", async () => {
      const { projectsCollection } = createCollections();

      const transaction = projectsCollection.deleteTransaction({
        id: "does-not-exist",
      });

      // actions are performed in the pre-commit, so we need to commit this before checking
      await transaction.commit();

      expect(transaction.secondaryOperations.length).toEqual(0);
    });

    it.concurrent(
      "Does nothing if foreign reference value is null",
      async () => {
        const { projectsCollection, usersCollection } = createCollections();

        await usersCollection.insert(user1);
        await projectsCollection.insert({
          ...project1,
          userId: null,
        });

        const transaction = projectsCollection.deleteTransaction({
          id: project1.id,
        });

        // actions are performed in the pre-commit, so we need to commit this before checking
        await transaction.commit();

        // no one should be decremented
        expect(transaction.secondaryOperations.length).toEqual(0);
      }
    );

    it.concurrent("Decrements foreign item if referenced", async () => {
      const { usersCollection, projectsCollection } = createCollections();

      await usersCollection.insert(user1);
      await projectsCollection.insert(project1);

      const preDeleteUser = await usersCollection.retrieveOne({
        where: { id: user1.id },
      });
      expect(preDeleteUser?.numProjects).toEqual(1);
      expect(preDeleteUser?.numActiveProjects).toEqual(1);
      expect(preDeleteUser?.numInactiveProjects).toEqual(0);

      const transaction = projectsCollection.deleteTransaction({
        id: project1.id,
      });
      await transaction.commit();

      // user count should be decremented
      expect(transaction.secondaryOperations.length).toEqual(1);

      const postDeleteUser = await usersCollection.retrieveOne({
        where: { id: user1.id },
      });
      expect(postDeleteUser?.numProjects).toEqual(0);
      expect(postDeleteUser?.numActiveProjects).toEqual(0);
      expect(postDeleteUser?.numInactiveProjects).toEqual(0);
    });
  });

  // TODO: implement & test migrations
});
