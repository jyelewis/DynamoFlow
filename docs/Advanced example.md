```typescript
import {DFTable} from 'dynamoflow';
import {DFSecondaryIndexExt} from "./DFSecondaryIndexExt";

// -------------------- 1. Create a DFTable for your DynamoDB Table --------------------
// following single table design, your application will likely only have one DFTable instance
// DynamoFlow will 
const table = new DFTable({
  // AWS credentials are loaded from the v3 SDK
  tableName: "my-application-table",
  GSIs: ["GSI1", "GSI2"], // GSIs can be added later if needed

  // using DynamoDBLocal
  endpoint: "http://localhost:8000",
});

// not recommended for production use, but useful for local development
await table.createTableIfNotExists();

// -------------------- 2. Create collections for each of your entity types ---------------

// any normal typescript type can be used
// DynamoFlow does not care about the schema of your objects
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
  partitionKey: "userId",
  sortKey: "id",
});


// more advanced collection example, using extensions to provide additional functionality
interface User {
  id: string;

  emailAddress: string;
  firstName: string;
  lastName: string;
  isActivated: boolean;

  numProjects: number;

  createdAt: string;
  modifiedAt: string;
}

const usersCollection = table.collection<User>({
  name: "users",

  // no sort key required here
  partitionKey: "id",

  // use extensions to provide additional functionality to this extension
  extensions: [
    // ensure users have a unique email address
    new DFUniqueFieldExt('emailAddress'),

    // keep track of when users are created and modified
    new DFModifiedTimesExt({
      createdAt: 'createdAt',
      modifiedAt: 'modifiedAt',
    }),

    // we'll need to look up users by email address when they sign in
    new DFSecondaryIndexExt({
      indexName: "byEmail",
      dynamoIndex: "GSI1",
      partitionKey: "emailAddress",
    }),

    // support searching for activated users by their last name
    new DFSecondaryIndexExt({
      indexName: "activeUsersNameSearch",
      dynamoIndex: "GSI2",
      includeInIndex: [
        user => user.isActivated,
        ['isActivated']
      ],
      partitionKey: "lastName",
      sortKey: "firstName",
    }),

    // keep track of how many projects each user has
    new DFForeignCountExt({
      collection: projectsCollection,
      foreignKey: 'userId',
      localKey: 'userId',
      localCountKey: 'numProjects',
    }),
  ],
});

// -------------------- 3. Use these collections to read & write from your table --------------------

// insert & update will always return the full entity
const user1 = await usersCollection.insert({
  id: "user-1",

  emailAddress: "joe@gmail.com",
  firstName: "Joe",
  lastName: "Bot",
  isActivated: false,

  numProjects: 0,

  createdAt: "",
  modifiedAt: ""
});

// mark our user as activated
await usersCollection.update({
  id: user1.id,
}, {
  isActivated: true
});

// create a project for our user
const user1sProject = await projectsCollection.insert({
  id: "project-1",
  userId: user1.id,

  name: "My First Project",
  description: "This is my first project",

  status: "DRAFT",
});

// search for activated users with the last name "Bot" and first name starting with "J"
const usersSearchResults = await usersCollection.query({
  index: "activeUsersNameSearch",
  where: {
    lastName: "Bot",
    firstName: {$beginsWith: "J"},
  }
});
/*
  usersSearchResults: [{
    id: "user-1",

    emailAddress: "joe@gmail.com",
    firstName: "Joe",
    lastName: "Bot",
    isActivated: true,
    
    numProjects: 1,
    
    createdAt: "2023-07-05T03:53:21.653Z",
    modifiedAt: "2023-07-05T03:53:21.653Z"
  }]
*/

```
