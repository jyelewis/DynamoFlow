import {
  EntityWithMetadata,
  Query,
  SafeEntity,
  UpdateValue,
} from "../types/types.js";
import { DFBaseExtension } from "./DFBaseExtension.js";
import { DFSecondaryIndexExt } from "./DFSecondaryIndexExt.js";
import { DFCollection } from "../DFCollection.js";
import { DFWriteTransaction } from "../DFWriteTransaction.js";
import { S2Cell, S2LatLng, S2LatLngRect, S2RegionCoverer } from "nodes2ts";

export interface GeoPoint {
  latitude: number;
  longitude: number;
}

export interface GeoQuery {
  near: GeoPoint;
  radiusMeters: number;
}

export interface DFGeospatialIndexExtConfig<Entity extends SafeEntity<Entity>> {
  indexName: string;
  dynamoIndex: "GSI1" | "GSI2" | "GSI3" | "GSI4" | "GSI5";
  partitionKey: (string & keyof Entity) | Array<string & keyof Entity>;
  pointField: keyof Entity;
}

// TODO: test me

export class DFGeospatialIndexExt<
  Entity extends SafeEntity<Entity>
> extends DFBaseExtension<Entity> {
  private secondaryIndex: DFSecondaryIndexExt<Entity>;
  public readonly geoHashFieldKey: string;
  public constructor(
    protected readonly config: DFGeospatialIndexExtConfig<Entity>
  ) {
    super();

    this.geoHashFieldKey = `_${this.config.indexName}_geohash`;

    this.secondaryIndex = new DFSecondaryIndexExt<Entity>({
      indexName: `${this.config.indexName}_geohash`,
      dynamoIndex: config.dynamoIndex,
      partitionKey: config.partitionKey,
      sortKey: this.geoHashFieldKey as any,
      includeInIndex: [
        (x: any) => x[this.geoHashFieldKey] !== null,
        [this.geoHashFieldKey as any],
      ],
    });
  }

  public init(collection: DFCollection<Entity>) {
    super.init(collection);

    this.secondaryIndex.init(this.collection);
  }

  public onInsert(entity: EntityWithMetadata, transaction: DFWriteTransaction) {
    const geoPoint = entity[this.config.pointField] as GeoPoint | null;
    entity[this.geoHashFieldKey] = this.generateGeohash(geoPoint);

    // let the secondary index extension do the heavy lifting of generating index keys
    this.secondaryIndex.onInsert(entity, transaction);
  }

  public onUpdate(
    key: Partial<Entity>,
    entityUpdate: Record<string, UpdateValue>,
    transaction: DFWriteTransaction
  ) {
    if (this.config.pointField in entityUpdate) {
      const geoPoint = entityUpdate[this.config.pointField] as GeoPoint | null;
      entityUpdate[this.geoHashFieldKey] = this.generateGeohash(geoPoint);
    }

    // let the secondary index extension do the heavy lifting of generating index keys
    this.secondaryIndex.onUpdate(key, entityUpdate, transaction);
  }

  public onQuery(query: Query<Entity>) {
    if (query.index !== this.config.indexName) {
      // user is not using this geo index
      return;
    }

    if (!(this.config.pointField in query.where)) {
      throw new Error(
        `Expected geospatial query to include ${
          this.config.pointField as string
        } in where clause`
      );
    }

    const geoQuery = query.where[this.config.pointField] as any as {
      $custom: GeoQuery;
    };
    if (
      typeof geoQuery !== "object" ||
      geoQuery === null ||
      !("$custom" in geoQuery)
    ) {
      throw new Error(
        `Invalid query for geospatial index ${this.config.indexName}`
      );
    }

    const centerLatLng = S2LatLng.fromDegrees(
      geoQuery.$custom.near.latitude,
      geoQuery.$custom.near.longitude
    );

    const latReferenceUnit = geoQuery.$custom.near.latitude > 0.0 ? -1.0 : 1.0;
    const latReferenceLatLng = S2LatLng.fromDegrees(
      geoQuery.$custom.near.latitude + latReferenceUnit,
      geoQuery.$custom.near.longitude
    );
    const lngReferenceUnit = geoQuery.$custom.near.longitude > 0.0 ? -1.0 : 1.0;
    const lngReferenceLatLng = S2LatLng.fromDegrees(
      geoQuery.$custom.near.latitude,
      geoQuery.$custom.near.longitude + lngReferenceUnit
    );

    const latForRadius =
      geoQuery.$custom.radiusMeters /
      centerLatLng.getEarthDistance(latReferenceLatLng);
    const lngForRadius =
      geoQuery.$custom.radiusMeters /
      centerLatLng.getEarthDistance(lngReferenceLatLng);

    const minLatLng = S2LatLng.fromDegrees(
      geoQuery.$custom.near.latitude - latForRadius,
      geoQuery.$custom.near.longitude - lngForRadius
    );
    const maxLatLng = S2LatLng.fromDegrees(
      geoQuery.$custom.near.latitude + latForRadius,
      geoQuery.$custom.near.longitude + lngForRadius
    );

    const latLngRect: S2LatLngRect = S2LatLngRect.fromLatLng(
      minLatLng,
      maxLatLng
    );

    const coveredCelIds = new S2RegionCoverer()
      .getCoveringCells(latLngRect)
      .map((x) => x.id.toNumber());

    const minCellId = Math.min(...coveredCelIds);
    const maxCellId = Math.max(...coveredCelIds);

    console.log(coveredCelIds, minCellId, maxCellId);

    // const results = await this.dispatchQueries(covering, queryRadiusInput);
    // return this.filterByRadius(results, queryRadiusInput);
    const whereWithCellIdRange: any = { ...query.where };
    whereWithCellIdRange[this.geoHashFieldKey] = {
      $betweenIncl: [minCellId.toString(), maxCellId.toString()],
    };
    // don't actually filter on the users geohash field
    whereWithCellIdRange[this.config.pointField] = undefined;

    const secondaryIndexQuery = {
      ...query,
      index: `${this.config.indexName}_geohash`,
      where: whereWithCellIdRange,
    };
    console.log("secondaryIndexQuery", secondaryIndexQuery, {
      $betweenIncl: [minCellId.toString(), maxCellId.toString()],
    });
    this.secondaryIndex.onQuery(secondaryIndexQuery);
    query.rawExpression = secondaryIndexQuery.rawExpression;
  }

  public filterQueryResults(
    query: Query<Entity>,
    entitiesWithMetadata: EntityWithMetadata[]
  ): EntityWithMetadata[] {
    // TODO: server side filtering here
    return entitiesWithMetadata;

    // let centerLatLng: S2LatLng = null;
    // let radiusInMeter = 0;
    //
    // const geoQuery.near: GeoPoint = (geoQueryInput as QueryRadiusInput).geoQuery.near;
    // centerLatLng = S2LatLng.fromDegrees(geoQuery.near.latitude, geoQuery.near.longitude);
    // radiusInMeter = (geoQueryInput as QueryRadiusInput).RadiusInMeter;
    //
    //
    // return list.filter(item => {
    //   const geoJson: string = item[this.config.geoJsonAttributeName].S;
    //   const coordinates = JSON.parse(geoJson).coordinates;
    //   const longitude = coordinates[this.config.longitudeFirst ? 0 : 1];
    //   const latitude = coordinates[this.config.longitudeFirst ? 1 : 0];
    //
    //   const latLng: S2LatLng = S2LatLng.fromDegrees(latitude, longitude);
    //   return (centerLatLng.getEarthDistance(latLng) as any).toNumber() <= radiusInMeter;
    // });
  }

  private generateGeohash(geoPoint: null | GeoPoint): null | string {
    // TODO: move these functions in and remove dependency on nodes2ts?
    if (geoPoint === null) {
      return null;
    }

    const latLng = S2LatLng.fromDegrees(geoPoint.latitude, geoPoint.longitude);
    const cell = S2Cell.fromLatLng(latLng);
    const cellId = cell.id;

    // TODO: need to 0 pad our string (I think)
    return cellId.id.toString();
  }
}
