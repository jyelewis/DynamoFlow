import { Query, SafeEntity } from "../types/types.js";
import { generateKeyString } from "./generateKeyString.js";
import { valueToSortableString } from "./valueToSortableString.js";
import { PartialQueryExpression } from "../types/internalTypes.js";
import { isDynamoValue } from "./isDynamoValue.js";

export function generateQueryExpression<Entity extends SafeEntity<Entity>>(
  collectionName: string,
  partitionKeys: string | string[],
  sortKeys: undefined | string | string[],
  query: Query<Entity>
): PartialQueryExpression {
  // will throw if required values are not provided
  const pk = `${collectionName}#${generateKeyString(
    partitionKeys,
    // casting as we assume PKs are all given as literals
    // if they aren't generateKeyString will throw
    query.where as any
  )}`;

  let queryIsComplete = false;
  let sortKeyBase = `${collectionName}`;

  // default query expression fetches all items in this partition
  // applying filters narrows this down
  const queryExpression: PartialQueryExpression = {
    keyConditionExpression: `#PK = :pk AND begins_with(#SK, :value)`,
    expressionAttributeValues: {
      ":pk": pk,
      ":value": `${collectionName}#`,
    },

    // default other properties
    // extensions can override this as possible
    expressionAttributeNames: {
      "#PK": "_PK",
      "#SK": "_SK",
    },
    indexName: undefined,
  };

  const sortKeysArr = Array.isArray(sortKeys)
    ? sortKeys
    : [sortKeys].filter(Boolean);
  for (const sortKey of sortKeysArr) {
    const sortValue = query.where[sortKey as keyof Entity];

    if (sortValue !== undefined && queryIsComplete) {
      throw new Error(
        `Cannot query on ${sortKey} after a range query or missing a previous parameter value`
      );
    }

    if (sortValue === undefined) {
      // we're done! not looking for anything further
      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if (
      typeof sortValue === "string" ||
      typeof sortValue === "number" ||
      typeof sortValue === "boolean" ||
      sortValue === null
    ) {
      // literal value, append to our search string
      sortKeyBase += `#${valueToSortableString(sortValue)}`;
      continue;
    }

    // non-literal, query values
    if (isDynamoValue(sortValue)) {
      throw new Error(
        `Invalid query where filter provided "${sortKey}": ${sortValue}`
      );
    }

    if ("$betweenIncl" in sortValue) {
      const gte = `${sortKeyBase}#${valueToSortableString(
        sortValue.$betweenIncl[0]
      )}#`;
      const lte = `${sortKeyBase}#${valueToSortableString(
        sortValue.$betweenIncl[1]
      )}#`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND #SK BETWEEN :gte AND :lte`;
      queryExpression.expressionAttributeValues[":gte"] = gte;
      queryExpression.expressionAttributeValues[":lte"] = lte;
      delete queryExpression.expressionAttributeValues[":value"]; // clear the "default" value prop

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if ("$beginsWith" in sortValue) {
      const value = `${sortKeyBase}#${valueToSortableString(
        sortValue.$beginsWith
      )}`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND begins_with(#SK, :value)`;
      queryExpression.expressionAttributeValues[":value"] = value;

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if ("$gt" in sortValue) {
      const value = `${sortKeyBase}#${valueToSortableString(sortValue.$gt)}#`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND #SK > :value`;
      queryExpression.expressionAttributeValues[":value"] = value;

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if ("$gte" in sortValue) {
      const value = `${sortKeyBase}#${valueToSortableString(sortValue.$gte)}#`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND #SK >= :value`;
      queryExpression.expressionAttributeValues[":value"] = value;

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if ("$lt" in sortValue) {
      const value = `${sortKeyBase}#${valueToSortableString(sortValue.$lt)}#`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND #SK < :value`;
      queryExpression.expressionAttributeValues[":value"] = value;

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    if ("$lte" in sortValue) {
      const value = `${sortKeyBase}#${valueToSortableString(sortValue.$lte)}#`;

      // store the final query we need to send
      queryExpression.keyConditionExpression = `#PK = :pk AND #SK <= :value`;
      queryExpression.expressionAttributeValues[":value"] = value;

      queryIsComplete = true;
      continue; // continue to check the dev hasn't provided any more values though
    }

    // how'd they get here?
    throw new Error(
      `Invalid query where filter provided "${sortKey}": ${JSON.stringify(
        sortValue
      )}`
    );
  }

  if (!queryIsComplete) {
    // mustn't have used any range type queries
    // update the selector to use baseSortKey
    queryExpression.keyConditionExpression = `#PK = :pk AND begins_with(#SK, :value)`;
    queryExpression.expressionAttributeValues[":value"] = `${sortKeyBase}#`;
  }

  return queryExpression;
}
