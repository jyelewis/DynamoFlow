export class WriteTransactionFailedError extends Error {
  constructor(public readonly error: string | Error) {
    // TODO: what is the point of this error exactly?
    super(`Write transaction failed: ${error}`);
  }
}
