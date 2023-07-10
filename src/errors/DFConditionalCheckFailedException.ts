import { DFWriteTransaction } from "../DFWriteTransaction.js";

export class DFConditionalCheckFailedException extends Error {
  constructor(transaction: DFWriteTransaction) {
    super(`Conditional check failed!\n${transaction.toString()}`);
  }
}
