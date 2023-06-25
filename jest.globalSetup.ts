import {
  testDbConfig,
  testFullTableScanDbConfig,
} from "./src/testHelpers/testDbConfigs.js";
import { DFDB } from "./src/DFDB.js";

async function jestGlobalSetup() {
  // create required test tables
  await Promise.all([
    new DFDB(testDbConfig).createTableIfNotExists(),
    new DFDB(testFullTableScanDbConfig).createTableIfNotExists(),
  ]);
}
module.exports = jestGlobalSetup;
