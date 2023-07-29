import { DFTable } from "../../DFTable.js";
import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import { DFForeignCountExt } from "../DFForeignCountExt.js";

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
}

const user1: User = {
  id: "user-1",
  name: "Joe Bloggs",
  numActiveProjects: 0,
  numInactiveProjects: 0,
  numProjects: 0,
};

// const user2: User = {
//   id: "user-2",
//   name: "Jane doe",
//   numActiveProjects: 0,
//   numInactiveProjects: 0,
//   numProjects: 0,
// };

const project1Active: Project = {
  id: "project-1",
  userId: user1.id,
  isActive: true,
};
// const project1Inactive: Project = {
//   id: "project-2",
//   userId: user1.id,
//   isActive: false,
// };

const createCollections = () => {
  const table = new DFTable(testDbConfigWithPrefix());
  const projectsCollection = table.createCollection<Project>({
    name: "projects",
    partitionKey: "userId",
    sortKey: "id",
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
        foreignCollection: projectsCollection,
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
  it("Installs remote extension", async () => {
    const { projectsCollection } = createCollections();

    // we have 3 counts against this collection
    expect(projectsCollection.extensions.length).toEqual(3);
  });

  describe("onInsert", () => {
    it("Does nothing if there is no relevant foreign item", async () => {
      const { projectsCollection } = createCollections();

      const transaction = projectsCollection.insertTransaction({
        ...project1Active,
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

      const transaction = projectsCollection.insertTransaction(project1Active);

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
    it.todo("Does nothing if no count dependent have been updated");

    // many test: 'null -> null', 'value -> null' ... etc
  });

  describe("onDelete", () => {
    it.todo("Does nothing if there is no foreign reference");
    it.todo("Decrements foreign item if referenced");
  });

  // TODO: implement & test migrations
});
