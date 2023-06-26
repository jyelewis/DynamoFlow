import { generateKeyString } from "../generateKeyString.js";

const testObject = {
  userId: "asdf-qwer",
  email: "joe@gmail.com",
  firstName: "Joe",
  lastName: "Bot",
  age: 53,
  dateCreated: 1687072800885,
  someFloat: 12345.689123,
  isActivated: true,
  isFreeTier: false,
};

describe("generateKeyString", () => {
  it("Single fields", () => {
    expect(generateKeyString(["userId"], testObject)).toMatchInlineSnapshot(
      `"asdf-qwer#"`
    );
    expect(generateKeyString(["email"], testObject)).toMatchInlineSnapshot(
      `"joe@gmail.com#"`
    );
  });

  it("Multiple fields", () => {
    expect(
      generateKeyString(["firstName", "lastName"], testObject)
    ).toMatchInlineSnapshot(`"joe#bot#"`);
    expect(
      generateKeyString(["userId", "email"], testObject)
    ).toMatchInlineSnapshot(`"asdf-qwer#joe@gmail.com#"`);
  });

  it("Supports providing a single key rather than an array", () => {
    expect(generateKeyString("userId", testObject)).toMatchInlineSnapshot(
      `"asdf-qwer#"`
    );
    expect(generateKeyString("email", testObject)).toMatchInlineSnapshot(
      `"joe@gmail.com#"`
    );
    expect(generateKeyString("isActivated", testObject)).toMatchInlineSnapshot(
      `"1#"`
    );
  });

  it("Throws if a key is missing", () => {
    expect(() => generateKeyString("userId", {})).toThrow(
      "Missing required value for userId in index"
    );
  });
});
