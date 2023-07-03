import {
  testDbConfig,
  testFullTableScanDbConfig,
} from "./src/testHelpers/testDbConfigs.js";
import { DFTable } from "./src/DFTable.js";

async function jestGlobalSetup() {
  // use mock AWS keys (required present for DDB Local)
  process.env.AWS_ACCESS_KEY_ID = "XXXXXXXXXXXXX";
  process.env.AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXX";

  // Ah-ha, I think this is running multiple times for multiple threads
  // create required test tables
  // stupid trick to ensure all our test threads don't race this and try to create the table twice
  await Promise.all([
    new DFTable(testDbConfig).createTableIfNotExists(),
    new DFTable(testFullTableScanDbConfig).createTableIfNotExists(),
  ]);
}
module.exports = jestGlobalSetup;
