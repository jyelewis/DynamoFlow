# Release notes

# 1.4.2
 * Update all dependencies
 * Reformat all code against prettier 3.0.1
 * Add security policy to repository 

# 1.4.1
 * Allow non-async preCommitHandlers
 * Fixed bug where postRetrieve handler wasn't provided entity metadata if the entity had just been migrated
 * Skip writing changes during migration, if the entity has not changed
 * migrateEntityWithMetadata now returns an EntityWithMetadata, rather than the stripped Entity

# 1.4.0
 * New DFForeignCountExt available

# 1.3.2
 * Confirmed https://github.com/jyelewis/DynamoFlow/issues/12 could not be exploited

# 1.3.1
 * Fixed bug in complex query expressions: https://github.com/jyelewis/DynamoFlow/issues/14

# 1.3.0
 * Experimental extension `DFForeignCountExt` now supports migrations
 * Fixed double hash SK issue: https://github.com/jyelewis/DynamoFlow/issues/4

## 1.2.2
## 1.2.1
 * CI Configuration

## 1.2.0
 * DFWriteTransaction now automatically merges operations on the same item within the same transaction
 * Removed dependency on `@aws-sdk/util-dynamodb` and `zod`
 * Added experimental extension `DFForeignCountExt` (still in progress)

## 1.1.0
 * More tests for DFUniqueConstraint
 * Bug fix for DFWriteTransaction

## 1.0.0
 * Initial release
