# Configuring DynamoDB tables

If you are not using `.createIfNotExists()`, you will need to configure your DynamoDB tables before using DynaFlow.

Keys must follow particular patterns (note capitalisation)
 * Partition key: "_PK"
 * Sort key: "_SK"
 * Ttl: "_ttl" (if using DFTtlExt)
 * GSI partition key: "_[gsi name]_PK" (i.e "_GSI1_PK")
 * GSI sort key: "_[gsi name]_SK" (i.e "_GSI1_SK")
   * We recommend generic names such as "GSI1", "GSI2", "GSI3" (as many as needed) and overloading these indexes across different collections
