// core
export { DFWriteTransaction } from "./DFWriteTransaction.js";
export { DFTable, DFTableConfig } from "./DFTable.js";
export { DFCollection, DFCollectionConfig } from "./DFCollection.js";

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

// TODO: will this throw if importing from an environment without Zod installed?
export { DFZodValidationExt } from "./extensions/DFZodValidationExt.js";
