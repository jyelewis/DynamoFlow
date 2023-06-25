export class WriteTransactionFailedError extends Error {
  constructor(public readonly error: string | Error) {
    super(`Write transaction failed: ${error}`);
  }
}
