import z from "zod";
import { DFTable } from "../../DFTable.js";
import { DFSecondaryIndexExt } from "../../extensions/DFSecondaryIndexExt.js";
import { DFUniqueConstraintExt } from "../../extensions/DFUniqueConstraintExt.js";
import { DFTimestampsExt } from "../../extensions/DFTimestampsExt.js";
import { DFZodValidationExt } from "../../extensions/DFZodValidationExt.js";

// create a single table for our app
const table = new DFTable({
  tableName: "messaging_app",
  GSIs: ["GSI1", "GSI2"],
});

// define our models & the collections that will house them

// -------------------------------------- Users --------------------------------------
export const userSchema = z.object({
  id: z.string(),

  // users message each other by phone number
  // their phone number may change, but their userId will not
  // some users may not have a phone number
  // these users can start a conversation, but not be messaged directly
  phoneNumber: z.string().nullable(),

  displayName: z.string(),
  profilePhoto: z.string().url().nullable(),
  email: z.string().email(),

  createdAt: z.string().datetime().optional(),
  updatedAt: z.string().datetime().optional(),
});
export type User = z.infer<typeof userSchema>;

export const usersCollection = table.createCollection<User>({
  name: "users",
  partitionKey: "id",
  extensions: [
    new DFZodValidationExt({ schema: userSchema }),
    new DFTimestampsExt({
      createdAtField: "createdAt",
      updatedAtField: "updatedAt",
    }),

    new DFSecondaryIndexExt({
      indexName: "byPhoneNumber",
      partitionKey: "phoneNumber",
      includeInIndex: [(user) => user.phoneNumber !== null, ["phoneNumber"]],
      dynamoIndex: "GSI1",
    }),
    new DFUniqueConstraintExt("displayName"),
    new DFUniqueConstraintExt("phoneNumber"),
    new DFUniqueConstraintExt("email"),
  ],
});

// -------------------------------------- Conversations --------------------------------------
export const conversationSchema = z.object({
  id: z.string(),

  // only used if this is a group conversation
  name: z.string().nullable(),
  displayPhoto: z.string().url().nullable(),

  createdAt: z.string().datetime().optional(),
  updatedAt: z.string().datetime().optional(),
});
export type Conversation = z.infer<typeof conversationSchema>;

export const conversationsCollection = table.createCollection<Conversation>({
  name: "conversations",
  partitionKey: "id",
  extensions: [
    new DFZodValidationExt({ schema: conversationSchema }),
    new DFTimestampsExt({
      createdAtField: "createdAt",
      updatedAtField: "updatedAt",
    }),
  ],
});

// -------------------------------------- ConversationParticipants --------------------------------------
export const conversationParticipantSchema = z.object({
  conversationId: z.string(),
  userId: z.string(),

  // denormalised values!
  // the last message time can be used to sort a users conversations by most recent
  lastMessageTime: z.string().datetime(),

  createdAt: z.string().datetime().optional(),
  updatedAt: z.string().datetime().optional(),
});
export type ConversationParticipant = z.infer<
  typeof conversationParticipantSchema
>;

export const conversationParticipantsCollection =
  table.createCollection<ConversationParticipant>({
    name: "conversationParticipants",

    partitionKey: "conversationId",
    sortKey: "userId",

    extensions: [
      new DFZodValidationExt({ schema: conversationParticipantSchema }),
      new DFTimestampsExt({
        createdAtField: "createdAt",
        updatedAtField: "updatedAt",
      }),
      new DFSecondaryIndexExt({
        // allow users to find all the conversations they are in
        // ordered by the last message they received

        // TODO: this is going to cause many double writes (changing the lastMessage field)
        indexName: "byUser",
        partitionKey: "userId",
        sortKey: "lastMessageTime",
        dynamoIndex: "GSI1",
      }),
    ],
  });

// -------------------------------------- Messages --------------------------------------
export const messageSchema = z.object({
  type: z.enum(["msg", "sys"]),
  text: z.string(),

  conversationId: z.string(),
  userId: z.string().nullable(), // system messages have no user ID
  sentAt: z.string().datetime(),

  createdAt: z.string().datetime().optional(),
  updatedAt: z.string().datetime().optional(),
});
export type Message = z.infer<typeof messageSchema>;

export const messagesCollection = table.createCollection<Message>({
  name: "messages",
  partitionKey: "conversationId",
  sortKey: ["sentAt", "userId"],
  extensions: [
    new DFZodValidationExt({ schema: messageSchema }),
    new DFTimestampsExt({
      createdAtField: "createdAt",
      updatedAtField: "updatedAt",
    }),
  ],
});
