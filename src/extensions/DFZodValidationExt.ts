import { EntityWithMetadata, SafeEntity, UpdateValue } from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import z from "zod";
import { isDynamoValue } from "../utils/isDynamoValue.js";

export class DFZodValidationExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  constructor(
    public readonly config: {
      schema: z.ZodObject<any>;
      // these default to true
      validateOnInsert?: boolean;
      validateOnUpdate?: boolean;
      validateOnRetrieve?: boolean;
    }
  ) {
    super();

    // default all validations to true
    this.config = {
      validateOnInsert: true,
      validateOnUpdate: true,
      validateOnRetrieve: true,
      ...this.config,
    };

    if (
      (!this.config.validateOnInsert || !this.config.validateOnUpdate) &&
      this.config.validateOnRetrieve
    ) {
      // we can't really handle this case
      // as any insert/update will use the retrieve processor to return a value to the user
      throw new Error(
        "If validateOnRetrieve is enabled, validateOnInsert & validateOnUpdate must also be enabled"
      );
    }
  }

  public onInsert(
    entityWithMetadata: EntityWithMetadata,
    transaction: DFWriteTransaction
  ) {
    if (!this.config.validateOnInsert) {
      // insert validation disabled
      return;
    }

    this.validateAndParse(entityWithMetadata);
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ) {
    if (!this.config.validateOnUpdate) {
      return;
    }

    // we can only parse the values we have for the update
    Object.entries(entityUpdate).forEach(([key, value]) => {
      if (key.startsWith("_")) {
        // ignore metadata fields
        return;
      }

      if (isDynamoValue(value)) {
        // parse new value (this also validates)
        entityUpdate[key] = this.config.schema.shape[key].parse(value);
      }

      // we have a dynamic update, we can't validate this
    });
  }

  public postRetrieve(entityWithMetadata: EntityWithMetadata): void {
    if (!this.config.validateOnRetrieve) {
      return;
    }

    this.validateAndParse(entityWithMetadata);
  }

  private validateAndParse(entityWithMetadata: EntityWithMetadata) {
    // strip metadata so we get the fields the user is updating
    const entity: any = {};
    Object.keys(entityWithMetadata)
      .filter((x) => !x.startsWith("_"))
      .forEach(
        (key) => (entity[key] = entityWithMetadata[key as keyof Entity])
      );

    // this will both validate the entity & clean up any non standard values for type (i.e 1 => true)
    const parsedEntity = this.config.schema.parse(entity);

    // pop the parsed fields back onto the entityWithMetadata (should be a 1:1 overwrite)
    Object.keys(parsedEntity).forEach((key) => {
      entityWithMetadata[key] = parsedEntity[key];
    });
  }
}
