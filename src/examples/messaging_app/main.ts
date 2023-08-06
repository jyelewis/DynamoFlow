import {
  Conversation,
  conversationParticipantsCollection,
  conversationsCollection,
  Message,
  messagesCollection,
  User,
  usersCollection,
} from "./db.js";
import { ulid } from "ulid";

export async function createUser(partialUser: {
  phoneNumber: null | string;
  displayName: string;
  email: string;
}): Promise<User> {
  return await usersCollection.insert({
    id: ulid(),
    profilePhoto: null,
    ...partialUser,
  });
}

export async function createConversation(newConversation: {
  name: null | string;
  participantUserIds: string[];
}): Promise<Conversation> {
  const now = new Date().toISOString();
  const conversationId = ulid();

  // create a new conversation
  const transaction = conversationsCollection.insertTransaction({
    id: conversationId,
    name: newConversation.name,
    displayPhoto: null,
  });

  // also create an initial message
  transaction.addSecondaryTransaction(
    messagesCollection.insertTransaction({
      conversationId,
      sentAt: now,

      type: "sys",
      userId: null,
      text: "Conversation created",
    })
  );

  // also add all provided users to this conversation
  for (const participantUserId of newConversation.participantUserIds) {
    transaction.addSecondaryTransaction(
      conversationParticipantsCollection.insertTransaction({
        conversationId,
        userId: participantUserId,

        lastMessageTime: now,
      })
    );
  }

  return (await transaction.commitWithReturn()) as Conversation;
}

export async function sendMessage({
  conversationId,
  userId,
  text,
}: {
  conversationId: string;
  userId: string;
  text: string;
}): Promise<Message> {
  // this is a write heavy operation
  // this schema is designed to make viewing conversations & messages very cheap
  // at the cost here, of creating conversations & sending messages being expensive

  // we've chosen to use this schema to demonstrate some of the features of DynamoFlow

  const transaction = messagesCollection.insertTransaction({
    conversationId,
    userId,
    sentAt: new Date().toISOString(),

    type: "msg",
    text,
  });

  // fetch the conversation participants
  const conversationParticipants =
    await conversationParticipantsCollection.retrieveMany({
      where: {
        conversationId,
      },
    });

  // update the conversation participants
  for (const participant of conversationParticipants) {
    transaction.addSecondaryTransaction(
      conversationParticipantsCollection.updateTransaction(
        {
          conversationId,
          userId: participant.userId,
        },
        {
          lastMessageTime: new Date().toISOString(),
        }
      )
    );
  }

  return (await transaction.commitWithReturn()) as Message;
}

export async function getRecentConversationsForUser(
  userId: string
): Promise<Conversation[]> {
  const userConversations =
    await conversationParticipantsCollection.retrieveMany({
      index: "byUser",
      where: {
        userId,
      },
      limit: 3,
      sort: "DESC",
    });

  if (userConversations.length === 0) {
    return [];
  }

  // fetch the underlying conversation object
  // probably not the best data structure - but demonstrates retrieveBatch ordering :)
  return await conversationsCollection.retrieveBatch(
    userConversations.map((uc) => ({ id: uc.conversationId }))
  );
}

export async function getMessagesForConversation(
  conversationId: string,
  limit?: number,
  before?: string
): Promise<Message[]> {
  console.log("Where", {
    conversationId,
    sentAt: before ? { $lt: before } : undefined,
  });

  return await messagesCollection.retrieveMany({
    where: {
      conversationId,
      sentAt: before ? { $lt: before } : undefined,
    },
    limit,
    sort: "DESC", // list newest messages first
  });
}
