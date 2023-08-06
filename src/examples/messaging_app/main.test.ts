import { testDbConfigWithPrefix } from "../../testHelpers/testDbConfigs.js";
import {
  createConversation,
  createUser,
  getMessagesForConversation,
  getRecentConversationsForUser,
  sendMessage,
} from "./main.js";
import {
  Conversation,
  conversationParticipantsCollection,
  User,
  usersCollection,
} from "./db.js";

// force the tables in the example to use the test config
jest.mock("../../DFTable.js", () => {
  class TestDFTable extends jest.requireActual("../../DFTable.js").DFTable {
    public constructor() {
      super(testDbConfigWithPrefix());
    }
  }

  return { DFTable: TestDFTable };
});

describe("Example: Messaging App", () => {
  let user1: User;
  let user2: User;
  let user3: User;
  let user4: User;

  let conversation12: Conversation;
  let conversation23: Conversation;
  let conversation123: Conversation;

  describe("Creates users", () => {
    it("Creates users", async () => {
      user1 = await createUser({
        phoneNumber: "+15555555555",
        displayName: "John Doe",
        email: "john.doe@gmail.com",
      });
      user2 = await createUser({
        phoneNumber: "+15555555556",
        displayName: "Jane Doe",
        email: "jane.doe@gmail.com",
      });
      user3 = await createUser({
        phoneNumber: "+15555555557",
        displayName: "Sally Smith",
        email: "s.smith@outlook.com",
      });
      user4 = await createUser({
        phoneNumber: "+15555555558",
        displayName: "Jony Ive",
        email: "design@apple.com",
      });

      const fetchedUser = await usersCollection.retrieveOne({
        where: {
          id: user1.id,
        },
      });
      expect(fetchedUser).toEqual({
        id: expect.any(String),

        phoneNumber: "+15555555555",

        displayName: "John Doe",
        email: "john.doe@gmail.com",
        profilePhoto: null,

        createdAt: expect.any(String),
        updatedAt: expect.any(String),
      });
    });

    it("Doesn't allow users with duplicate phone numbers", async () => {
      await expect(
        createUser({
          phoneNumber: "+15555555555",
          displayName: "Fake user",
          email: "fake.email@gmail.com",
        })
      ).rejects.toThrow("Unique constraint violation on field 'phoneNumber'");
    });

    it("Doesn't allow users with duplicate email addresses", async () => {
      await expect(
        createUser({
          phoneNumber: null,
          displayName: "Fake user",
          email: "john.doe@gmail.com",
        })
      ).rejects.toThrow("Unique constraint violation on field 'email'");
    });

    it("Doesn't allow users with duplicate display names", async () => {
      await expect(
        createUser({
          phoneNumber: null,
          displayName: "John Doe",
          email: "fake.email@gmail.com",
        })
      ).rejects.toThrow("Unique constraint violation on field 'displayName'");
    });
  });

  it("Throws if creating a user with invalid data", async () => {
    await expect(
      createUser({
        displayName: "Broken user",
        email: 123 as any, // should be a string
        phoneNumber: null,
      })
    ).rejects.toThrow("Expected string, received number");
  });

  describe("Creates conversations", () => {
    it("Creates a conversation between user 1 & 2", async () => {
      conversation12 = await createConversation({
        name: "1&2",
        participantUserIds: [user1.id, user2.id],
      });

      expect(conversation12).toEqual({
        id: expect.any(String),
        name: "1&2",
        displayPhoto: null,

        createdAt: expect.any(String),
        updatedAt: expect.any(String),
      });

      // check the participants were added
      const conversation12Participants =
        await conversationParticipantsCollection.retrieveMany({
          where: {
            conversationId: conversation12.id,
          },
        });

      expect(conversation12Participants).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            conversationId: conversation12.id,
            userId: user1.id,
          }),
          expect.objectContaining({
            conversationId: conversation12.id,
            userId: user2.id,
          }),
        ])
      );
    });

    it("Creates a conversation between 2 & 3", async () => {
      conversation23 = await createConversation({
        name: "2&3",
        participantUserIds: [user2.id, user3.id],
      });
    });

    it("Creates a conversation between 1, 2 & 3", async () => {
      conversation123 = await createConversation({
        name: "1, 2 & 3",
        participantUserIds: [user2.id, user3.id, user1.id],
      });
    });
  });

  it("Users can see their recent conversations (ordered by creation/latest message)", async () => {
    const user1Conversations = await getRecentConversationsForUser(user1.id);
    expect(user1Conversations).toEqual([conversation123, conversation12]);

    const user2Conversations = await getRecentConversationsForUser(user2.id);
    expect(user2Conversations).toEqual([
      conversation123,
      conversation23,
      conversation12,
    ]);

    const user3Conversations = await getRecentConversationsForUser(user3.id);
    expect(user3Conversations).toEqual([conversation123, conversation23]);
  });

  it("Can send messages", async () => {
    await sendMessage({
      conversationId: conversation12.id,
      userId: user1.id,
      text: "Hello!",
    });

    await sendMessage({
      conversationId: conversation12.id,
      userId: user2.id,
      text: "Hey there",
    });

    await sendMessage({
      conversationId: conversation12.id,
      userId: user2.id,
      text: "Hows it going?",
    });

    await sendMessage({
      conversationId: conversation12.id,
      userId: user2.id,
      text: "Yeah not too bad",
    });
  });

  it("Can read messages in a conversation", async () => {
    const c12Messages = await getMessagesForConversation(conversation12.id);
    expect(c12Messages.map((msg) => msg.text)).toEqual([
      // messages are sorted newest -> oldest
      "Yeah not too bad",
      "Hows it going?",
      "Hey there",
      "Hello!",
      "Conversation created",
    ]);

    const c23Messages = await getMessagesForConversation(conversation23.id);
    expect(c23Messages.map((msg) => msg.text)).toEqual([
      "Conversation created",
    ]);
  });

  it("Can paginate backwards through a conversation", async () => {
    const c12LastMessages = await getMessagesForConversation(
      conversation12.id,
      2
    );
    expect(c12LastMessages.map((msg) => msg.text)).toEqual([
      "Yeah not too bad",
      "Hows it going?",
    ]);

    const oldestMessage = c12LastMessages[c12LastMessages.length - 1];

    // search for 2 more messages, older than the last oldest message we had
    const c12SecondLastMessages = await getMessagesForConversation(
      conversation12.id,
      2,
      oldestMessage.sentAt
    );

    expect(c12SecondLastMessages.map((msg) => msg.text)).toEqual([
      "Hey there",
      "Hello!",
    ]);
  });

  it("Adjusts conversation ordering based on last message sent", async () => {
    const user1Conversations = await getRecentConversationsForUser(user1.id);
    expect(user1Conversations).toEqual([conversation12, conversation123]);

    const user2Conversations = await getRecentConversationsForUser(user2.id);
    expect(user2Conversations).toEqual([
      conversation12,
      conversation123,
      conversation23,
    ]);

    const user3Conversations = await getRecentConversationsForUser(user3.id);
    expect(user3Conversations).toEqual([conversation123, conversation23]);

    const user4Conversations = await getRecentConversationsForUser(user4.id);
    expect(user4Conversations).toEqual([]);
  });
});
