import { DynamoValue } from "../types/types.js";

export function valueToSortableString(value: DynamoValue): string {
  if (value === null) {
    // sort null items first (capital N to sort before lowercase prefixes)
    // this could conflict with strings containing "!NULL!"
    return "!NULL!";
  }

  if (typeof value === "string") {
    // escape hash values & prefix with "s:"
    return `${value.toLowerCase().replace(/#/g, "\\#").substring(0, 100)}`;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const isNegative = (value as number) < 0;
    // fixed format: "xxxxxxxxxx.yyyyyy" (both as base 36 numbers)
    // this allows us to string sort these floats

    const integerSize = 10; // more than enough to hold a unix ms timestamp
    const decimalPlaces = 6;

    const integerValue = Math.floor(value);
    const decimalValue = value - integerValue;

    const integerString36 = Math.abs(integerValue)
      .toString(36)
      .padStart(integerSize, "0")
      .substring(0, integerSize);
    const decimalString10 = decimalValue
      .toPrecision(decimalPlaces)
      .substring(2, 2 + decimalPlaces);
    const decimalString36 = parseInt(decimalString10, 10)
      .toString(36)
      .padStart(decimalPlaces, "0")
      .substring(0, decimalPlaces);

    return `${isNegative ? "-" : ""}${integerString36}.${decimalString36}`;
  }

  // sort true items first
  if (value === true) {
    return `1`;
  }
  if (value === false) {
    return `0`;
  }

  throw new Error(
    `Value of type ${typeof value} is not a string, number, boolean or null`
  );
}
