import { zodSchemaToDFSchema } from "../DFZodSchemaExt.js";
import { z } from "zod";

describe("DFZodSchemaExt", () => {
  it.skip("Defines schema for collection", () => {
    // const userSchema = z.object({
    //   id: z.string(),
    //   firstName: z.string(),
    //   lastName: z.string(),
    //   age: z.number().nullable(),
    // });

    // const ext = new DFZodSchemaExt(userSchema);

    expect(1).toEqual(1);
    // TODO: test something
  });

  // TODO: test whole module
});

describe("zodSchemaToDFSchema", () => {
  it("Converts basic schema", () => {
    const userSchema = z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      age: z.number().nullable(),
    });

    const dfSchema = zodSchemaToDFSchema(userSchema);

    expect(dfSchema).toEqual({
      id: {
        type: "string",
        nullable: false,
      },
      firstName: {
        type: "string",
        nullable: false,
      },
      lastName: {
        type: "string",
        nullable: false,
      },
      age: {
        type: "number",
        nullable: true,
      },
    });
  });

  it("Converts advanced schema", () => {
    const userSchema = z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      dateJoined: z.string().datetime(),
      age: z.number().nullable(),
      recentlyVisitedPages: z.array(z.string()).nullable(),
      preferences: z.object({
        theme: z.string(),
        isDarkMode: z.boolean(),
      }),
    });

    const dfSchema = zodSchemaToDFSchema(userSchema);

    expect(dfSchema).toEqual({
      id: {
        type: "string",
        nullable: false,
      },
      firstName: {
        type: "string",
        nullable: false,
      },
      lastName: {
        type: "string",
        nullable: false,
      },
      dateJoined: {
        type: "string",
        nullable: false,
      },
      age: {
        type: "number",
        nullable: true,
      },
      recentlyVisitedPages: {
        type: "array",
        nullable: true,
      },
      preferences: {
        type: "object",
        nullable: false,
      },
    });
  });
});
