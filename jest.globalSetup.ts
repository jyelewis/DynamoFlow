import {
  testDbConfig,
  testFullTableScanDbConfig,
} from "./src/testHelpers/testDbConfigs.js";
import { DFDB } from "./src/DFDB.js";

async function jestGlobalSetup() {
  // use mock AWS keys (required present for DDB Local)
  process.env.AWS_ACCESS_KEY_ID = "XXXXXXXXXXXXX";
  process.env.AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXX";

  // create required test tables
  await Promise.all([
    new DFDB(testDbConfig).createTableIfNotExists(),
    new DFDB(testFullTableScanDbConfig).createTableIfNotExists(),
  ]);
}
module.exports = jestGlobalSetup;
