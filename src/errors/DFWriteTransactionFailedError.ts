import { DFWriteTransaction } from "../DFWriteTransaction.js";

export class DFWriteTransactionFailedError extends Error {
  constructor(
    transaction: DFWriteTransaction,
    public readonly error: string | Error
  ) {
    super(`Write transaction failed: ${error}\n\n${transaction.toString()}`);
  }
}
