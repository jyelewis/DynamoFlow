import { DFBaseExtension } from "../DFBaseExtension.js";

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
});
