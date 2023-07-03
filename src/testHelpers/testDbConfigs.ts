import { DFTableConfig } from "../DFTable.js";

// almost all tests should use this
export const testDbConfig: DFTableConfig = {
  region: "ap-southeast-2",
  tableName: "DynamoFlow-tests-primary",
  endpoint: "http://localhost:8000",
  GSIs: ["GSI1", "GSI2"],
};

// kept separate intentionally to allow testing full table scans & migrations
// this table should not grow each time the tests are run as scans have a 1:1 relationship with the number of items in the table
export const testFullTableScanDbConfig: DFTableConfig = {
  region: "ap-southeast-2",
  tableName: "DynamoFlow-tests-full-table-scan",
  endpoint: "http://localhost:8000",
};

export const testCreatedInTestDbConfig: DFTableConfig = {
  region: "ap-southeast-2",
  tableName: "DynamoFlow-tests-created-in-test",
  endpoint: "http://localhost:8000",
};

export const testCreatedInTestGSIsDbConfig: DFTableConfig = {
  region: "ap-southeast-2",
  tableName: "DynamoFlow-tests-created-in-test-gsis",
  endpoint: "http://localhost:8000",
  GSIs: ["GSI1", "GSI2"],
};
