// used for extensions to specify how to query an index
export interface PartialQueryExpression {
  keyConditionExpression: string;
  expressionAttributeNames: {
    "#PK": string;
    "#SK": string;
  };
  expressionAttributeValues: Record<string, string>;
  indexName: string | undefined;
}
