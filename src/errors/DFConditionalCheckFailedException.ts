import { DFWriteTransaction } from "../DFWriteTransaction.js";

export class DFConditionalCheckFailedException extends Error {
  constructor(transaction: DFWriteTransaction) {
    // TODO: Similar error class for malformed requests
    super(`Conditional check failed!\n${transaction.toString()}`);
  }
}
