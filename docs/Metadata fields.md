# Metadata fields

DynamoFlow uses a number of metadata fields to store information about your data in DynamoDB.
Below is a list of metadata fields that may be written to your entities.
Extensions may also attach metadata to your entities.

Any field starting with an underscore is considered metadata and will be stripped before returning the entity from any operations.
This behaviour can be disabled by setting `returnRaw: true` when querying.

Known internal metadata fields
 - _PK
 - _SK
 - _GSI1PK (From DFSecondaryIndexExt)
 - _GSI1PK (From DFSecondaryIndexExt)

 - _c: collection name
 - _wc: write count
 - _ttl
 - _v: version (From DFMigrationExt)

