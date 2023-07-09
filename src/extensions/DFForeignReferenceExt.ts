import { SafeEntity } from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFCollection } from "../DFCollection.js";

// TODO: test this
// TODO: finish this, just a POC

// TODO: not totally sold on this as an idea, the user can always code this manually into their Dal delete method
//       and this could lead to some scary cascade transactions if there are many sub items (or sub-sub items)

export class DFForeignReferenceExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  constructor(
    public readonly reference: {
      localField: keyof Entity;
      // TODO: keyof foreign collection type
      foreignField: string;
      collection: DFCollection<any>;
      onForeignDelete: "CASCADE" | "RESTRICT" | "SET_NULL" | "IGNORE";
    }
  ) {
    super();
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // attach this reference onto the schema
    // TODO: do we want to auto populate these fields or only extend them?
    const refFieldSchema: any = this.collection.schema[
      this.reference.localField
    ] || {
      type: "string",
      nullable: true,
    };
    this.collection.schema[this.reference.localField] = refFieldSchema;

    refFieldSchema.references = {
      collection: this.reference.collection,
      foreignField: this.reference.foreignField,
    };
  }

  // TODO: implement foreign update checks (probs as a sidebar extension)

  // TODO: finish me :)
}
