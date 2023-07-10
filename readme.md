# DynaFlow

A practical & extendable DynamoDB client for TypeScript applications.

[![Coverage Status](https://coveralls.io/repos/github/jyelewis/dynaflow/badge.svg?branch=main)](https://coveralls.io/github/jyelewis/dynaflow?branch=main)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)


## Why?
...

## Features
...

## Getting Started

1. Install the package: `npm install --save dynaflow` or `yarn add dynaflow`
2. Start DynamoDB Local `docker run -p 8000:8000 amazon/dynamodb-local` (or copy our example [docker-compose.yml](docker-compose.yml) file)
3. Create a [DFTable](docs/DFTable.md) instance
    1. Following [single table design](https://aws.amazon.com/blogs/database/single-table-vs-multi-table-design-in-amazon-dynamodb/), your application will likely only have one DFTable instance
    2. AWS credentials are loaded from the v3 SDK
```typescript
import {DFTable} from 'dynaflow';

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
    1. Any normal TypeScript type can be used, DynaFlow does not care about the schema of your objects
    2. If desired, you can use extensions like DFZodValidationExt to validate your objects at runtime
5. Create [collections](docs/DFCollection.md) for each entity type you have
    1. Collections are used to read & write from your table
    2. Many collections can exist within a single Table
    3. [Extensions](docs/Collection%20extensions.md) can be used to add additional functionality to your collections

```typescript

interface User {
  id: string;

  name: string;
  email: string;
}

const usersCollection = table.createCollection<User>({
  name: "users",
  partitionKey: "id",
  extensions: [
    new DFUniqueFieldExt('email')
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

6. Use these [collections](docs/DFCollection.md) to read & write from your [table](docs/DFTable.md)
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

