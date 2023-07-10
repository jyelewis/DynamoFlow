import { EntityWithMetadata, SafeEntity } from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import z from "zod";

// TODO: finish this, just a POC
// TODO: can we make Zod not a hard dependency?

// interface DFSecondaryIndexExtConfig<Entity extends SafeEntity<Entity>> {
//   indexName: string;
//   dynamoIndex: "GSI1" | "GSI2" | "GSI3" | "GSI4" | "GSI5";
//   partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
//   sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
//   includeInIndex?: [(entity: Entity) => boolean, Array<keyof Entity>];
// }

export class DFZodValidationExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  // TODO: need more settings
  //  1. Allow cleanup
  //  2. validateOnRetrieve
  //  3. validateOnInsert
  //  4. validateOnUpdate
  constructor(public readonly zodSchema: z.ZodObject<any>) {
    super();
  }

  // TODO: where in the chain does this sit?
  public onInsert(
    entityWithMetadata: EntityWithMetadata,
    transaction: DFWriteTransaction
  ) {
    // TODO: do this on retrieve & update as well

    // strip metadata so we get the fields the user is updating
    const entity: any = {};
    Object.keys(entityWithMetadata)
      .filter((x) => !x.startsWith("_"))
      .forEach(
        (key) => (entity[key] = entityWithMetadata[key as keyof Entity])
      );

    // this will both validate the entity & clean up any non standard values for type (i.e 1 => true)
    const parsedEntity = this.zodSchema.parse(entity);

    // pop the parsed fields back onto the entityWithMetadata (should be a 1:1 overwrite)
    Object.keys(parsedEntity).forEach((key) => {
      entityWithMetadata[key] = parsedEntity[key];
    });
  }

  // TODO: finish me :)
}
