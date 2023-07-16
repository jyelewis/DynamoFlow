# DynamoFlow

A practical & extendable DynamoDB client for TypeScript applications.

[![Coverage Status](https://coveralls.io/repos/github/jyelewis/DynamoFlow/badge.svg?branch=main)](https://coveralls.io/github/jyelewis/DynamoFlow?branch=main)
[![NPM](https://img.shields.io/npm/v/badges.svg)](https://www.npmjs.com/package/dynamoflow)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)


## Features
* Supports & encourages [single table design](https://aws.amazon.com/blogs/database/single-table-vs-multi-table-design-in-amazon-dynamodb/)
* Abstracts your different item types into [collections](https://github.com/jyelewis/DynamoFlow/blob/main/docs/DFCollection.md)
* [Transactions](https://github.com/jyelewis/DynamoFlow/blob/main/src/DFWriteTransaction.ts)
* [Unique constraints](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFUniqueConstraintExt.ts)
* [Secondary indexes](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFSecondaryIndexExt.ts)
* [Timestamping](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFTimestampsExt.ts)
* [Zod validation](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFZodValidationExt.ts)
* [TTL](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFTtlExt.ts)
* Extendable via [extensions](https://github.com/jyelewis/DynamoFlow/blob/main/docs/Collection%20extensions.md)
* On the fly schema [migrations](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFMigrationExt.ts)
* Scan / batch schema [migrations](https://github.com/jyelewis/DynamoFlow/blob/main/src/extensions/DFMigrationExt.ts)

## Why?
Unlike many other database technologies, DynamoDB expects your data management logic to live in the application layer.
Rather than providing built-in features such as unique fields, foreign keys, or cascading deletes,
DynamoDB provides the foundational technologies to implement these features yourself.

Rather than picking which of these features we do and don't include, DynamoFlow provides a set of tools to make it easy to implement these features yourself.
We also provide a set of ready-to-go extensions for common patters, such as unique fields, foreign keys, secondary indexes & timestamping

There are several other Typescript DynamoDB clients available, and I encourage you to check them all out before committing to one.

My personal favourites are
 * [dynamodb-toolbox](https://github.com/jeremydaly/dynamodb-toolbox)
 * [TypeDORM](https://github.com/typedorm/typedorm)
 * [AWS lib-dynamodb](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/)


## Getting Started

1. Install the package: `npm install --save dynamoflow` or `yarn add dynamoflow`
2. Start DynamoDB Local `docker run -p 8000:8000 amazon/dynamodb-local` (or copy our example [docker-compose.yml](docker-compose.yml) file)
3. Create a [DFTable](https://github.com/jyelewis/DynamoFlow/blob/main/docs/DFTable.md) instance
    1. Following [single table design](https://aws.amazon.com/blogs/database/single-table-vs-multi-table-design-in-amazon-dynamodb/), your application will likely only have one DFTable instance
    2. AWS credentials are loaded from the v3 SDK
```typescript
import {DFTable} from "dynamoflow";

const table = new DFTable({
  tableName: "my-application-table",
  GSIs: ["GSI1", "GSI2"], // GSIs can be added later if needed

  // using DynamoDBLocal
  endpoint: "http://localhost:8000",
});

// not recommended for production use, but useful for local development
await table.createTableIfNotExists();
```

4. Define types for your entities
    1. Any normal TypeScript type can be used, DynamoFlow does not care about the schema of your objects
    2. If desired, you can use extensions like DFZodValidationExt to validate your objects at runtime
5. Create [collections](https://github.com/jyelewis/DynamoFlow/blob/main/docs/DFCollection.md) for each entity type you have
    1. Collections are used to read & write from your table
    2. Many collections can exist within a single Table
    3. [Extensions](https://github.com/jyelewis/DynamoFlow/blob/main/docs/Collection%20extensions.md) can be used to add additional functionality to your collections

```typescript
import {DFUniqueConstraintExt} from "dynamoflow";

interface User {
   id: string;

   name: string;
   email: string;
}

const usersCollection = table.createCollection<User>({
   name: "users",
   partitionKey: "id",
   extensions: [
      new DFUniqueConstraintExt('email')
   ],
});

interface Project {
   id: string;
   userId: string;

   name: string;
   description: string;

   status: "DRAFT" | "IN-PROGRESS" | "COMPLETED";
}

const projectsCollection = table.createCollection<Project>({
   // the name of the collection is used to prefix the partition key for each item
   name: "projects",

   // any string, number or boolean fields of this entity can be used as keys
   // different collections can have different keys
   partitionKey: "userId",
   sortKey: "id"
});

```

6. Use these [collections](https://github.com/jyelewis/DynamoFlow/blob/main/docs/DFCollection.md) to read & write from your [table](https://github.com/jyelewis/DynamoFlow/blob/main/docs/DFTable.md)
```typescript
const user1 = await usersCollection.insert({
    id: "user-1",
    name: "John Smith",
    email: "john.s@gmail.com"
});

const insertedProject = await projectsCollection.insert({
  id: "project-1",
  userId: user1.id,

  name: "My First Project",
  description: "This is my first project",

  status: "DRAFT",
});

await projectsCollection.update({
  id: "project-1"
}, {
  status: "IN-PROGRESS"
});

const retrievedProject = await projectsCollection.retrieveOne({
  where: {
    userId: "user-1",
    id: "project-1"
  }
});
```

For a more comprehensive example, take a look at the included [Messaging app](https://github.com/jyelewis/DynamoFlow/blob/main/src/examples/messaging_app) demo.

Once you're ready to test against a real DynamoDB table, read [Configuring DynamoDB Tables](https://github.com/jyelewis/DynamoFlow/blob/main/docs/Configuring%20DynamoDB%20tables.md)
