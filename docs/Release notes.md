# Release notes

## 1.2.0
 * DFWriteTransaction now automatically merges operations on the same item within the same transaction
 * Removed dependency on `@aws-sdk/util-dynamodb` and `zod`
 * Added experimental extension `DFForeignCountExt` (still in progress)

## 1.1.0
 * More tests for DFUniqueConstraint
 * Bug fix for DFWriteTransaction

## 1.0.0
 * Initial release
