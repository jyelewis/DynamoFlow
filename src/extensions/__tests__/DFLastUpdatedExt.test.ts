import { DFTable } from "../../DFTable.js";
import { testDbConfig } from "../../testHelpers/testDbConfigs.js";
import { genTestPrefix } from "../../testHelpers/genTestPrefix.js";
import { DFLastUpdatedExt } from "../DFLastUpdatedExt.js";

interface User {
  id: number;
  firstName: string;
  lastName: string;
  isActivated: boolean;
  age?: number;
  lastUpdated?: string;
}
describe("DFLastUpdatedExt", () => {
  it.concurrent("Populates last updated field on insert", async () => {
    const table = new DFTable(testDbConfig);

    const usersCollection = table.createCollection<User>({
      name: `${genTestPrefix()}-user`,
      partitionKey: "id",
      extensions: [new DFLastUpdatedExt("lastUpdated")],
    });

    const insertedUser = await usersCollection.insert({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
    });

    expect(insertedUser).toEqual({
      id: 1,
      firstName: "Jye",
      lastName: "Lewis",
      isActivated: true,
      lastUpdated: expect.any(String),
    });
  });
});
