import { EntityWithMetadata, SafeEntity } from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import z from "zod";
import { DFCollection, DFCollectionSchema } from "../DFCollection.js";

// TODO: finish this, just a POC
// TODO: can we make Zod not a hard dependency?

// interface DFSecondaryIndexExtConfig<Entity extends SafeEntity<Entity>> {
//   indexName: string;
//   dynamoIndex: "GSI1" | "GSI2" | "GSI3" | "GSI4" | "GSI5";
//   partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
//   sortKey?: (string & keyof Entity) | Array<string & keyof Entity>;
//   includeInIndex?: [(entity: Entity) => boolean, Array<keyof Entity>];
// }

export class DFZodSchemaExt<
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

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    // convert the zod schema into a DF schema & store on the collection
    // TODO: probs don't want to over-write everything..
    this.collection.schema = zodSchemaToDFSchema(
      this.zodSchema
    ) as DFCollectionSchema<Entity>;
  }

  // TODO: where in the chain does this sit?
  public onInsert(
    entityWithMetadata: EntityWithMetadata,
    transaction: DFWriteTransaction
  ) {
    // TODO: do this on retrieve as well

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

export function zodSchemaToDFSchema(
  zodSchema: z.ZodObject<any>
): DFCollectionSchema<any> {
  // TODO: throw if a schema cannot be converted (i.e optional root properties or unknown types)

  const schema: DFCollectionSchema<any> = {};

  // wedge our Entity schema keys into this,
  // runtime zod validation will check the keys are actually correct
  Object.entries(zodSchema.shape).forEach(([key, def]) => {
    let nullable = false;

    let defType = (def as any)._def.typeName;
    if (defType === "ZodNullable") {
      def = (def as z.ZodNullable<any>).unwrap();
      defType = (def as any)._def.typeName;
      nullable = true;
    }

    // can't use instanceof, different zod packages so types may not align
    if (defType === "ZodString") {
      schema[key] = {
        type: "string",
        nullable,
      };
      return;
    }

    if (defType === "ZodNumber") {
      schema[key] = {
        type: "number",
        nullable,
      };
      return;
    }

    if (defType === "ZodBoolean") {
      schema[key] = {
        type: "boolean",
        nullable,
      };
      return;
    }

    if (defType === "ZodEnum") {
      // const map = def.enum;
      console.log("enum:", def);
      schema[key] = {
        type: "string",
        allowedValues: [1, 2, 3], // TODO figure this out
        nullable,
      };
      return;
    }

    if (defType === "ZodObject") {
      schema[key] = {
        type: "object",
        nullable,
      };
      return;
    }

    if (defType === "ZodArray") {
      schema[key] = {
        type: "array",
        nullable,
      };
      return;
    }

    console.log(defType, def);
    throw new Error("Unknown zod type");
  });

  return schema;
}
