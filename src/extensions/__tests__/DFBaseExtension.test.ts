import { DFBaseExtension } from "../DFBaseExtension.js";
import { DFTable } from "../../DFTable.js";
import { testDbConfig } from "../../testHelpers/testDbConfigs.js";

class MyExt extends DFBaseExtension<any> {
  public fetchCollection() {
    return this.collection;
  }
}

describe("DFBaseExtension", () => {
  it("Throws if collection is accessed before init", () => {
    const ext = new MyExt();
    expect(() => ext.fetchCollection()).toThrowError(
      "Collection not set, had this extension been init()'ed?"
    );
  });

  const ext = new MyExt();

  // init the extension
  const table = new DFTable(testDbConfig);
  table.createCollection<any>({
    name: "test",
    partitionKey: "id",
    extensions: [ext],
  });

  describe("All base functions work", () => {
    it("onInsert()", () => {
      const transaction = table.createTransaction({
        type: "Update",
        key: {},
        updateValues: {},
      });
      ext.onInsert({}, transaction);
    });

    it("onUpdate()", () => {
      const transaction = table.createTransaction({
        type: "Update",
        key: {},
        updateValues: {},
      });
      ext.onUpdate({}, {}, transaction);
    });

    it("onUpdate()", () => {
      const transaction = table.createTransaction({
        type: "Update",
        key: {},
        updateValues: {},
      });
      ext.onUpdate({}, {}, transaction);
    });

    it("onDelete()", () => {
      const transaction = table.createTransaction({
        type: "Delete",
        key: {},
      });
      ext.onDelete({}, transaction);
    });

    it("expressionForQuery()", () => {
      expect(
        ext.expressionForQuery({
          where: {},
        })
      ).toEqual(undefined);
    });

    it("onQuery()", () => {
      expect(
        ext.onQuery({
          where: {},
        })
      ).toEqual(undefined);
    });

    it("entityRequiresMigration()", () => {
      expect(ext.entityRequiresMigration({})).toEqual(false);
    });

    it("onUpdate()", () => {
      const transaction = table.createTransaction({
        type: "Update",
        key: {},
        updateValues: {},
      });
      ext.migrateEntity({}, transaction);
    });

    it("postRetrieve()", () => {
      ext.postRetrieve({});
    });
  });
});
