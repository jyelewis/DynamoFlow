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
    expect(generateKeyString(["userId"], testObject)).toEqual(
      "userId=s:asdf-qwer#"
    );
    expect(generateKeyString(["email"], testObject)).toEqual(
      "email=s:joe@gmail.com#"
    );
  });

  it("Multiple fields", () => {
    expect(generateKeyString(["firstName", "lastName"], testObject)).toEqual(
      "firstName=s:joe#lastName=s:bot#"
    );
    expect(generateKeyString(["userId", "email"], testObject)).toEqual(
      "userId=s:asdf-qwer#email=s:joe@gmail.com#"
    );
  });

  it("Supports providing a single key rather than an array", () => {
    expect(generateKeyString("userId", testObject)).toEqual(
      "userId=s:asdf-qwer#"
    );
    expect(generateKeyString("email", testObject)).toEqual(
      "email=s:joe@gmail.com#"
    );
    expect(generateKeyString("isActivated", testObject)).toEqual(
      "isActivated=b:1#"
    );
  });

  it("Throws if a key is missing", () => {
    expect(() => generateKeyString("userId", {})).toThrow(
      "Missing required value for userId in index"
    );
  });
});
