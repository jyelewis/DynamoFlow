# Getting started

## Installation

Install the package: `npm install dynaflow` or `yarn add dynaflow`

```typescript
// -------------------- 1. Create a DFTable for your DynamoDB Table --------------------
// following single table design, your application will likely only have one DFTable instance
const table = new DFTable({
  // AWS credentials are loaded from the v3 SDK
  tableName: "my-application-table",
  GSIs: ["GSI1", "GSI2"], // GSIs can be added later if needed

  // using DynamoDBLocal
  endpoint: "http://localhost:8000",
});

// -------------------- 2. Create collections for each of your entity types ---------------

// any normal typescript type can be used
// Dynaflow does not care about the schema of your objects
// if desired, you can use extensions like DFZodValidationExt to validate your objects at runtime
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
  partitionKey: "id",
});

// -------------------- 3. Use these collections to read & write from your table --------------------

// create a project
const insertedProject = await projectsCollection.insert({
  id: "project-1",

  name: "My First Project",
  description: "This is my first project",

  status: "DRAFT",
});

// update a project
await projectsCollection.update({
  id: "project-1"
}, {
  status: "IN-PROGRESS"
});

// retreive a project
const retrievedProject = await projectsCollection.retrieveOne({
  where: {
    id: "project-1"
  }
});

```
