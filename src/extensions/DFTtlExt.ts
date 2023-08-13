import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import {
  DynamoValue,
  EntityWithMetadata,
  Query,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { isDynamoValue } from "../utils/isDynamoValue.js";
import { DFCondition } from "../types/operations.js";

export class DFTtlExt<
  Entity extends SafeEntity<Entity>,
> extends DFBaseExtension<Entity> {
  public constructor(
    public readonly config: {
      expiresAtField: keyof Entity;
      filterExpired?: boolean;
    },
  ) {
    super();
  }

  // convert any JS date into a unix timestamp for dynamos TTL feature
  public dateValueToDynamoTtl(dateValue: DynamoValue): number {
    // only accept types we can convert to a timestamp, and can persist in Dynamo (ISO strings or ms time)
    if (typeof dateValue !== "string" && typeof dateValue !== "number") {
      throw new Error("Invalid date provided to DFTtlExt");
    }

    const dateObj = new Date(dateValue);

    const time = dateObj.getTime();
    if (isNaN(time)) {
      throw new Error("Invalid date provided to DFTtlExt");
    }

    if (time < Date.now()) {
      throw new Error("Date provided to DFTtlExt is in the past");
    }

    // Dynamo works with unix timestamps (seconds)
    return Math.floor(time / 1000);
  }

  public onInsert(
    entity: EntityWithMetadata,
    transaction: DFWriteTransaction,
  ): void {
    const expiryFieldValue = entity[this.config.expiresAtField as string];
    if (expiryFieldValue) {
      entity._ttl = this.dateValueToDynamoTtl(expiryFieldValue);
    }
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction,
  ): void {
    const expiryFieldUpdateValue =
      entityUpdate[this.config.expiresAtField as string];
    if (expiryFieldUpdateValue === undefined) {
      // no update to expiry field, do nothing
      return;
    }

    if (
      (expiryFieldUpdateValue !== null &&
        typeof expiryFieldUpdateValue === "object" &&
        "$remove" in expiryFieldUpdateValue) ||
      !expiryFieldUpdateValue
    ) {
      // user wants to remove the expiry field
      // either they've explicitly used the $remove operator, or they've set it to a falsey value
      // remove the TTL as well
      entityUpdate._ttl = { $remove: true };
      return;
    }

    if (!isDynamoValue(expiryFieldUpdateValue)) {
      throw new Error(
        `TTL field '${
          this.config.expiresAtField as string
        }' cannot accept dynamic updates`,
      );
    }

    entityUpdate._ttl = this.dateValueToDynamoTtl(expiryFieldUpdateValue);
  }

  public onQuery(query: Query<Entity>): DFCondition | undefined {
    if (!this.config.filterExpired) {
      return undefined;
    }

    // user wants to filter out expired entities
    // DynamoDB doesn't instantly delete expired entities
    // so some records may be retrieved for a period of time after they've expired
    // 'filterExpired' lets the user opt-in to filtering out items that have expired, but still exist in the table

    query.filter = query.filter || {};

    query.filter[this.config.expiresAtField] = {
      $raw: {
        conditionExpression: "attribute_not_exists(#ttl) OR #ttl >= :ttl_now",
        expressionAttributeNames: {
          "#ttl": "_ttl",
        },
        expressionAttributeValues: {
          ":ttl_now": this.dateValueToDynamoTtl(Date.now()),
        },
      },
    };
  }
}
