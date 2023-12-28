// core
export { DFWriteTransaction } from "./DFWriteTransaction.js";
export { DFTable, DFTableConfig } from "./DFTable.js";
export { DFCollection, DFCollectionConfig } from "./DFCollection.js";

// types & errors
export {
  RETRY_TRANSACTION,
  DynamoValue,
  UpdateValue,
  DynamoItem,
  EntityWithMetadata,
  STOP_SCAN,
  FullTableScan,
  SafeEntity,
  FullTableScanItem,
  Query,
} from "./types/types.js";
export { DFConditionalCheckFailedError } from "./errors/DFConditionalCheckFailedError.js";
export { DFWriteTransactionFailedError } from "./errors/DFWriteTransactionFailedError.js";

// built-in extensions
export { DFBaseExtension } from "./extensions/DFBaseExtension.js";
export {
  DFMigrationExt,
  DFMigrationExtConfig,
} from "./extensions/DFMigrationExt.js";
export {
  DFSecondaryIndexExt,
  DFSecondaryIndexExtConfig,
} from "./extensions/DFSecondaryIndexExt.js";
export { DFTimestampsExt } from "./extensions/DFTimestampsExt.js";
export { DFTtlExt } from "./extensions/DFTtlExt.js";
export {
  DFUniqueConstraintExt,
  DFUniqueConstraintConflictError,
} from "./extensions/DFUniqueConstraintExt.js";
export { DFZodValidationExt } from "./extensions/DFZodValidationExt.js";
export {
  DFForeignCountExt,
  DFForeignCountExtConfig,
} from "./extensions/DFForeignCountExt.js";
