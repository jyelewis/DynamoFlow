export class DFConditionalCheckFailedException extends Error {
  constructor() {
    // TODO: could this take and print the transaction it was trying to commit?
    // TODO: Similar error class for malformed requests
    super(`Conditional check failed`);
  }
}
