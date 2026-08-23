/**
 * Shared Parquet helpers — flatten property records and build Parquet buffers.
 * Extracted from publish-query-table.ts for reuse in ingest.ts.
 */

import { writeFile, readFile, unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { PropertyRecord } from './types.js';

// ---------------------------------------------------------------------------
// Flatten property record to tabular format
// ---------------------------------------------------------------------------

export function flattenProperty(prop: PropertyRecord): Record<string, unknown> {
  return {
    uuid: prop.uuid,
    parcel_id: prop.parcel_id,
    street: prop.address?.street ?? null,
    address_city: prop.address?.city ?? null,
    state: prop.address?.state ?? null,
    address_zip: prop.address?.zip ?? null,
    full_address: prop.address?.full ?? null,
    county_jurisdiction: prop.county_jurisdiction,
    assessed_value: prop.assessed_value,
    market_value: prop.market_value,
    current_owner_name: prop.current_owner?.owner_name ?? null,
    current_owner_type: prop.current_owner?.owner_type ?? null,
    year_built: prop.structure?.year_built ?? null,
    sqft: prop.structure?.sqft ?? null,
    stories: prop.structure?.stories ?? null,
    bedrooms: prop.structure?.bedrooms ?? null,
    bathrooms: prop.structure?.bathrooms ?? null,
    roof_type: prop.structure?.roof_type ?? null,
    construction_type: prop.structure?.construction_type ?? null,
    use_code: prop.structure?.use_code ?? null,
    use_description: prop.structure?.use_description ?? null,
    lot_area_sqft: prop.lot?.area_sqft ?? null,
    lot_area_acres: prop.lot?.area_acres ?? null,
    zoning: prop.lot?.zoning ?? null,
    lat: prop.coordinates?.lat ?? null,
    lng: prop.coordinates?.lng ?? null,
    taxable_value: prop.tax?.taxable_value ?? null,
    tax_year: prop.tax?.tax_year ?? null,
    annual_tax: prop.tax?.annual_tax ?? null,
    // Derived signals as top-level columns
    roof_age_years: prop.derived_signals?.roof_age_years ?? null,
    ownership_tenure_years: prop.derived_signals?.ownership_tenure_years ?? null,
    is_regional_owner: prop.derived_signals?.is_regional_owner ?? null,
    water_proximity_ft: prop.derived_signals?.water_proximity_ft ?? null,
    is_waterfront: prop.derived_signals?.is_waterfront ?? null,
    transit_distance_mi: prop.derived_signals?.transit_distance_mi ?? null,
    starbucks_distance_mi: prop.derived_signals?.starbucks_distance_mi ?? null,
    within_walking_transit: prop.derived_signals?.within_walking_transit ?? null,
    within_walking_starbucks: prop.derived_signals?.within_walking_starbucks ?? null,
    // Provenance summary
    source_count: prop.provenance?.contributing_sources?.length ?? 0,
    reconciliation_confidence: prop.provenance?.reconciliation_confidence ?? null,
    provenance_last_run: prop.provenance?.last_pipeline_run ?? null,
    provenance_sources: prop.provenance?.contributing_sources?.join(', ') ?? null,
    provenance_timestamps: prop.provenance?.collection_timestamps
      ? JSON.stringify(prop.provenance.collection_timestamps)
      : null,
  };
}

// ---------------------------------------------------------------------------
// Build Parquet buffer from flat rows
// ---------------------------------------------------------------------------

export async function buildParquetBuffer(rows: Record<string, unknown>[]): Promise<Buffer> {
  const parquetModule = await import('parquetjs-lite');
  // parquetjs-lite is CJS — dynamic import wraps it: { default: { ParquetSchema, ParquetWriter, ... } }
  const parquet = parquetModule.default ?? parquetModule;

  const schema = new parquet.ParquetSchema({
    uuid: { type: 'UTF8' },
    parcel_id: { type: 'UTF8' },
    street: { type: 'UTF8', optional: true },
    address_city: { type: 'UTF8', optional: true },
    state: { type: 'UTF8', optional: true },
    address_zip: { type: 'UTF8', optional: true },
    full_address: { type: 'UTF8', optional: true },
    county_jurisdiction: { type: 'UTF8' },
    assessed_value: { type: 'DOUBLE', optional: true },
    market_value: { type: 'DOUBLE', optional: true },
    current_owner_name: { type: 'UTF8', optional: true },
    current_owner_type: { type: 'UTF8', optional: true },
    year_built: { type: 'INT32', optional: true },
    sqft: { type: 'INT32', optional: true },
    stories: { type: 'INT32', optional: true },
    bedrooms: { type: 'INT32', optional: true },
    bathrooms: { type: 'INT32', optional: true },
    roof_type: { type: 'UTF8', optional: true },
    construction_type: { type: 'UTF8', optional: true },
    use_code: { type: 'UTF8', optional: true },
    use_description: { type: 'UTF8', optional: true },
    lot_area_sqft: { type: 'DOUBLE', optional: true },
    lot_area_acres: { type: 'DOUBLE', optional: true },
    zoning: { type: 'UTF8', optional: true },
    lat: { type: 'DOUBLE', optional: true },
    lng: { type: 'DOUBLE', optional: true },
    taxable_value: { type: 'DOUBLE', optional: true },
    tax_year: { type: 'INT32', optional: true },
    annual_tax: { type: 'DOUBLE', optional: true },
    roof_age_years: { type: 'INT32', optional: true },
    ownership_tenure_years: { type: 'INT32', optional: true },
    is_regional_owner: { type: 'BOOLEAN', optional: true },
    water_proximity_ft: { type: 'DOUBLE', optional: true },
    is_waterfront: { type: 'BOOLEAN', optional: true },
    transit_distance_mi: { type: 'DOUBLE', optional: true },
    starbucks_distance_mi: { type: 'DOUBLE', optional: true },
    within_walking_transit: { type: 'BOOLEAN', optional: true },
    within_walking_starbucks: { type: 'BOOLEAN', optional: true },
    source_count: { type: 'INT32', optional: true },
    reconciliation_confidence: { type: 'DOUBLE', optional: true },
    provenance_last_run: { type: 'UTF8', optional: true },
    provenance_sources: { type: 'UTF8', optional: true },
    provenance_timestamps: { type: 'UTF8', optional: true },
  });

  // parquetjs-lite only has openFile/openStream — write to temp file, read back
  const tmpPath = join(tmpdir(), `query-table-${randomUUID()}.parquet`);
  const writer = await parquet.ParquetWriter.openFile(schema, tmpPath);

  for (const row of rows) {
    const cleanRow: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(row)) {
      if (val !== null && val !== undefined) {
        cleanRow[key] = val;
      }
    }
    await writer.appendRow(cleanRow);
  }

  await writer.close();

  const buffer = await readFile(tmpPath);
  await unlink(tmpPath).catch(() => {}); // cleanup
  return buffer;
}
