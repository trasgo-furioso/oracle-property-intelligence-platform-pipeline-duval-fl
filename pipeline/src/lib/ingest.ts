/**
 * Core ingestion function — extracted from pilot-ingest.ts for reuse.
 * Called by the CLI script and the POST /api/runs/trigger route.
 */

import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getPool, runMigrations } from './db.js';

// Filebase / IPNS
import { uploadJson, uploadParquet, bucket as filebaseBucket, getCid, KEY_PREFIX } from './filebase.js';
import { upsertName, IPNS_LABEL } from './ipns.js';

// Parquet helpers
import { flattenProperty, buildParquetBuffer } from './parquet-helpers.js';

import type { PropertyRecord } from './types.js';

// Source adapters (mock generators)
import { generateMockAppraiserRecord } from '../sources/appraiser.js';
import { generateMockPermitRecord } from '../sources/permits.js';
import { generateMockOwnershipRecord } from '../sources/ownership.js';
import { generateMockGeoRecord } from '../sources/geo.js';
import { generateMockBusinessRecord } from '../sources/business.js';
import { generateMockContractorRecord } from '../sources/contractor.js';
import { generateMockSunbizRecord } from '../sources/sunbiz.js';
import { generateMockBBBRecord } from '../sources/bbb.js';

// Real data source (FDOT statewide parcels — not geo-blocked)
import { fetchDuvalParcels, fetchParcelsByIds } from '../sources/fdot-parcels.js';

// Transforms
import { transformAppraiserRecords } from '../transforms/duval/appraiser-transform.js';
import { transformPermitRecords } from '../transforms/duval/permits-transform.js';
import { transformOwnershipRecords } from '../transforms/duval/ownership-transform.js';
import { transformGeoRecords } from '../transforms/duval/geo-transform.js';
import { transformBusinessRecords } from '../transforms/duval/business-transform.js';
import { transformContractorRecords } from '../transforms/duval/contractor-transform.js';
import { transformSunbizRecords } from '../transforms/duval/sunbiz-transform.js';
import { transformBBBRecords } from '../transforms/duval/bbb-transform.js';
import { transformFdotRecords } from '../transforms/duval/fdot-transform.js';

// Provenance
import { createProvenance, mergeProvenance } from './provenance.js';

// Feeder
import { feed } from './feeder.js';

import type {
  RawRecord,
  TransformResult,
  DeltaCounts,
  Provenance,
  DerivedSignals,
} from './types.js';

// ---------------------------------------------------------------------------
// Content hashing
// ---------------------------------------------------------------------------

function computeContentHash(fields: Partial<Record<string, unknown>>): string {
  const normalized = JSON.stringify(fields, Object.keys(fields).sort());
  return createHash('sha256').update(normalized).digest('hex').slice(0, 32);
}

// ---------------------------------------------------------------------------
// Source registry
// ---------------------------------------------------------------------------

interface SourceEntry {
  sourceId: string;
  name: string;
  category: 'property' | 'permit' | 'ownership' | 'business' | 'contractor' | 'location';
  url: string;
  collectionMethod: 'browser-flow' | 'api' | 'bulk-download' | 'scrape';
  mockGenerator: (parcelId: string) => RawRecord;
  transform: (records: RawRecord[]) => TransformResult[];
  /** If true, this source is ONLY used in mock mode (skipped in real-data mode) */
  mockOnly?: boolean;
}

/**
 * Mock source entries — used when USE_REAL_DATA is not set.
 * These generate fabricated data for development/testing.
 */
const MOCK_SOURCE_ENTRIES: SourceEntry[] = [
  { sourceId: 'duval-appraiser', name: 'Duval County Property Appraiser', category: 'property', url: 'https://paopropertysearch.coj.net/', collectionMethod: 'browser-flow', mockGenerator: generateMockAppraiserRecord, transform: transformAppraiserRecords },
  { sourceId: 'duval-permits', name: 'Duval County Permits', category: 'permit', url: 'https://buildinginspections.coj.net/', collectionMethod: 'browser-flow', mockGenerator: generateMockPermitRecord, transform: transformPermitRecords },
  { sourceId: 'duval-ownership', name: 'Duval County Ownership Records', category: 'ownership', url: 'https://paopropertysearch.coj.net/', collectionMethod: 'browser-flow', mockGenerator: generateMockOwnershipRecord, transform: transformOwnershipRecords },
  { sourceId: 'duval-geo', name: 'Duval County GIS', category: 'location', url: 'https://maps.coj.net/coj/rest/services/CityBiz/Parcels/MapServer', collectionMethod: 'api', mockGenerator: generateMockGeoRecord, transform: transformGeoRecords },
  { sourceId: 'duval-business', name: 'Duval County Business Tax Receipts', category: 'business', url: 'https://www.coj.net/departments/finance/business-tax-receipts', collectionMethod: 'browser-flow', mockGenerator: generateMockBusinessRecord, transform: transformBusinessRecords },
  { sourceId: 'duval-contractor', name: 'FL DBPR Contractor Licenses', category: 'contractor', url: 'https://www.myfloridalicense.com/wl11.asp', collectionMethod: 'scrape', mockGenerator: generateMockContractorRecord, transform: transformContractorRecords },
  { sourceId: 'duval-sunbiz', name: 'FL Sunbiz Corporate Registry', category: 'business', url: 'https://search.sunbiz.org/', collectionMethod: 'scrape', mockGenerator: generateMockSunbizRecord, transform: transformSunbizRecords },
  { sourceId: 'duval-bbb', name: 'BBB Business Profiles', category: 'business', url: 'https://www.bbb.org/', collectionMethod: 'scrape', mockGenerator: generateMockBBBRecord, transform: transformBBBRecords },
];

/**
 * Real-data source entry — COJ ArcGIS parcels (City of Jacksonville).
 * Provides real parcel data: addresses, valuations, coordinates, owner info, use codes.
 * Replaces mock appraiser + geo + ownership in real-data mode.
 */
const COJ_SOURCE_ENTRY: SourceEntry = {
  sourceId: 'coj-duval-parcels',
  name: 'City of Jacksonville ArcGIS Parcels (Duval County)',
  category: 'property',
  url: 'https://maps.coj.net/coj/rest/services',
  collectionMethod: 'api',
  // Mock generator is a no-op — real data is loaded from coj-parcels.json
  mockGenerator: (_parcelId: string) => ({
    parcel_id: _parcelId,
    source_id: 'coj-duval-parcels',
    raw_data: {},
  }),
  transform: transformFdotRecords,
};

/** @deprecated Use COJ_SOURCE_ENTRY. Kept for backward compatibility. */
const FDOT_SOURCE_ENTRY = COJ_SOURCE_ENTRY;

/**
 * Resolve the path to the pre-fetched real data file.
 * Checks for COJ data first (primary), then FDOT (legacy fallback).
 * Tries both relative-to-module and absolute /app/data paths (Docker container).
 */
function resolveRealDataPath(): string | null {
  const candidates: string[] = [];

  // Relative to compiled module (works in dev and most Docker setups)
  const base = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..', 'data', 'real');
  candidates.push(resolve(base, 'coj-parcels.json'));
  candidates.push(resolve(base, 'fdot-parcels.json'));

  // Absolute Docker container path (fallback)
  candidates.push('/app/data/real/coj-parcels.json');
  candidates.push('/app/data/real/fdot-parcels.json');

  for (const p of candidates) {
    if (existsSync(p)) {
      console.info(`  [real-data] Found real data file: ${p}`);
      return p;
    }
  }

  console.info(`  [real-data] No real data file found. Searched: ${candidates.join(', ')}`);
  return null;
}

/**
 * Check if real-data mode is enabled via USE_REAL_DATA env var.
 * Also auto-enables if pre-fetched real data files exist and PIPELINE_USE_MOCK is not set.
 */
function useRealData(): boolean {
  if (process.env.PIPELINE_USE_MOCK === '1' || process.env.PIPELINE_USE_MOCK === 'true') {
    return false;
  }
  if (process.env.USE_REAL_DATA === '1' || process.env.USE_REAL_DATA === 'true') {
    return true;
  }
  // Auto-detect: if real data files exist, use them
  return resolveRealDataPath() !== null;
}

/**
 * Try to load pre-fetched real data from pipeline/data/real/.
 * Checks for coj-parcels.json first (COJ ArcGIS), then fdot-parcels.json (legacy).
 * Returns null if no file exists or data is invalid.
 *
 * When COJ is the primary source, also tries to load FDOT data and merge
 * supplementary fields (year_built, total_living_area, etc.) by parcel_id.
 */
function loadPreFetchedFdotData(): RawRecord[] | null {
  const realDataPath = resolveRealDataPath();
  if (!realDataPath) return null;

  const isCoj = realDataPath.includes('coj-parcels');
  const sourceId = isCoj ? 'coj-duval-parcels' : 'fdot-duval-parcels';

  try {
    const raw = JSON.parse(readFileSync(realDataPath, 'utf-8')) as Array<Record<string, unknown>>;
    console.info(`  Loaded ${raw.length} pre-fetched parcels from ${realDataPath}`);

    // If COJ is primary, try to supplement with FDOT data for year_built, sqft, etc.
    let fdotByParcelId: Map<string, Record<string, unknown>> | null = null;
    if (isCoj) {
      fdotByParcelId = loadFdotSupplement();
    }

    // Also try loading year-built.json supplement (from PAO bulk or estimation)
    const yearBuiltMap = loadYearBuiltSupplement();

    // Convert to RawRecord format expected by fdot-transform
    // Use parcel_id field, falling back to re field (COJ uses 're' as parcel ID)
    return raw.map((r) => {
      const parcelId = String(r.parcel_id ?? r.re ?? '');
      const merged = { ...r };

      // Merge FDOT supplementary fields into COJ record if available
      if (fdotByParcelId) {
        // Normalize parcel ID for matching (remove spaces)
        const normalizedId = parcelId.replace(/\s+/g, '');
        const fdot = fdotByParcelId.get(normalizedId) ?? fdotByParcelId.get(parcelId);
        if (fdot) {
          // Only fill in fields that COJ doesn't have
          if (merged.year_built === undefined || merged.year_built === null) {
            merged.year_built = fdot.year_built ?? null;
          }
          if (merged.effective_year_built === undefined || merged.effective_year_built === null) {
            merged.effective_year_built = fdot.effective_year_built ?? null;
          }
          if (merged.total_living_area === undefined || merged.total_living_area === null) {
            merged.total_living_area = fdot.total_living_area ?? null;
          }
          if (merged.sale_date === undefined || merged.sale_date === null) {
            merged.sale_date = fdot.sale_date ?? null;
          }
          if (merged.sale_price === undefined || merged.sale_price === null) {
            merged.sale_price = fdot.sale_price ?? null;
          }
          if (merged.dor_use_code === undefined || merged.dor_use_code === null) {
            merged.dor_use_code = fdot.dor_use_code ?? null;
          }
        }
      }

      // Merge year-built supplement (from PAO bulk or estimation)
      if (yearBuiltMap && (merged.year_built === undefined || merged.year_built === null)) {
        const normalizedId = parcelId.replace(/\s+/g, '');
        const yb = yearBuiltMap.get(normalizedId) ?? yearBuiltMap.get(parcelId);
        if (yb !== undefined) {
          merged.year_built = yb;
        }
      }

      return {
        parcel_id: parcelId,
        source_id: sourceId,
        raw_data: merged,
      };
    });
  } catch (err) {
    console.warn(`  Failed to load pre-fetched data: ${err}`);
    return null;
  }
}

/**
 * Load FDOT supplementary data indexed by parcel_id.
 * Used to enrich COJ records with fields COJ doesn't provide (year_built, sqft, etc.).
 */
function loadFdotSupplement(): Map<string, Record<string, unknown>> | null {
  const base = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..', 'data', 'real');
  const candidates = [
    resolve(base, 'fdot-parcels.json'),
    '/app/data/real/fdot-parcels.json',
  ];

  for (const p of candidates) {
    if (!existsSync(p)) continue;
    try {
      const raw = JSON.parse(readFileSync(p, 'utf-8')) as Array<Record<string, unknown>>;
      const map = new Map<string, Record<string, unknown>>();
      for (const r of raw) {
        const id = String(r.parcel_id ?? '').replace(/\s+/g, '');
        if (id) map.set(id, r);
      }
      console.info(`  [real-data] Loaded ${map.size} FDOT supplement records from ${p}`);
      return map;
    } catch {
      continue;
    }
  }

  console.info(`  [real-data] No FDOT supplement data found (year_built will be missing for COJ-only parcels)`);
  return null;
}

/**
 * Load year-built supplement data from year-built.json.
 * Produced by fetch-year-built.ts (FDOT, PAO bulk, or estimation).
 */
function loadYearBuiltSupplement(): Map<string, number> | null {
  const base = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..', 'data', 'real');
  const candidates = [
    resolve(base, 'year-built.json'),
    '/app/data/real/year-built.json',
  ];

  for (const p of candidates) {
    if (!existsSync(p)) continue;
    try {
      const raw = JSON.parse(readFileSync(p, 'utf-8')) as Array<{ parcel_id: string; year_built: number }>;
      const map = new Map<string, number>();
      for (const r of raw) {
        const id = String(r.parcel_id ?? '').replace(/\s+/g, '');
        if (id && r.year_built) map.set(id, r.year_built);
      }
      console.info(`  [real-data] Loaded ${map.size} year-built supplement records from ${p}`);
      return map;
    } catch {
      continue;
    }
  }

  return null;
}

/**
 * Get the source entries based on the data mode.
 * In real-data mode, FDOT replaces mock appraiser/geo/ownership (it covers all three).
 * Mock-only sources (business, contractor, sunbiz, bbb) still generate mock data.
 */
function getSourceEntries(): SourceEntry[] {
  if (useRealData()) {
    return [
      COJ_SOURCE_ENTRY,
      // Keep mock generators for sources not covered by COJ parcels
      ...MOCK_SOURCE_ENTRIES.filter((e) =>
        ['duval-permits', 'duval-business', 'duval-contractor', 'duval-sunbiz', 'duval-bbb'].includes(e.sourceId),
      ),
    ];
  }
  return MOCK_SOURCE_ENTRIES;
}

// Legacy compatibility
const SOURCE_ENTRIES = MOCK_SOURCE_ENTRIES;

// ---------------------------------------------------------------------------
// Standalone loader (bypasses Restate for direct DB access)
// ---------------------------------------------------------------------------

async function loadRecordsDirect(
  runId: string,
  sourceId: string,
  records: TransformResult[],
): Promise<DeltaCounts> {
  const pool = getPool();
  let newCount = 0;
  let updatedCount = 0;

  for (const record of records) {
    const contentHash = computeContentHash(record.fields as Record<string, unknown>);
    const parcelId = record.parcel_id;

    // Check existing record (parcel_id is natural key — idempotency guard)
    const existing = await pool.query<{
      uuid: string;
      content_hash: string | null;
      provenance: Provenance;
      derived_signals: DerivedSignals;
    }>(
      'SELECT uuid, content_hash, provenance, derived_signals FROM properties WHERE parcel_id = $1',
      [parcelId],
    );

    if (existing.rows.length === 0) {
      const provenance = createProvenance(sourceId, runId);

      await pool.query(
        `INSERT INTO properties (
          parcel_id, address, county_jurisdiction,
          assessed_value, market_value, ownership, current_owner,
          permits, structure, lot, coordinates, tax,
          provenance, derived_signals, content_hash
        ) VALUES (
          $1, $2, 'duval',
          $3, $4, $5, $6,
          $7, $8, $9, $10, $11,
          $12, $13, $14
        )
        ON CONFLICT (parcel_id) DO NOTHING`,
        [
          parcelId,
          JSON.stringify(record.fields.address ?? {}),
          record.fields.assessed_value ?? null,
          record.fields.market_value ?? null,
          JSON.stringify(record.fields.ownership ?? []),
          JSON.stringify(record.fields.current_owner ?? null),
          JSON.stringify(record.fields.permits ?? []),
          JSON.stringify(record.fields.structure ?? {}),
          JSON.stringify(record.fields.lot ?? {}),
          JSON.stringify(record.fields.coordinates ?? null),
          JSON.stringify(record.fields.tax ?? {}),
          JSON.stringify(provenance),
          JSON.stringify(record.fields.derived_signals ?? {}),
          contentHash,
        ],
      );
      newCount++;
    } else {
      const row = existing.rows[0]!;

      // Content hash comparison — idempotency guard
      if (row.content_hash === contentHash) {
        continue; // No change, skip
      }

      const existingProvenance =
        typeof row.provenance === 'string'
          ? (JSON.parse(row.provenance) as Provenance)
          : (row.provenance as Provenance);

      const newProvenance = mergeProvenance(existingProvenance, sourceId, runId);

      const existingSignals =
        typeof row.derived_signals === 'string'
          ? (JSON.parse(row.derived_signals) as DerivedSignals)
          : (row.derived_signals as DerivedSignals);

      const mergedSignals = { ...existingSignals, ...record.fields.derived_signals };

      const updates: string[] = [];
      const values: unknown[] = [];
      let paramIdx = 1;

      const setField = (col: string, val: unknown) => {
        if (val !== undefined) {
          updates.push(`${col} = $${paramIdx}`);
          values.push(typeof val === 'object' ? JSON.stringify(val) : val);
          paramIdx++;
        }
      };

      if (record.fields.address) setField('address', record.fields.address);
      if (record.fields.assessed_value !== undefined) setField('assessed_value', record.fields.assessed_value);
      if (record.fields.market_value !== undefined) setField('market_value', record.fields.market_value);
      if (record.fields.ownership) setField('ownership', record.fields.ownership);
      if (record.fields.current_owner !== undefined) setField('current_owner', record.fields.current_owner);
      if (record.fields.permits) setField('permits', record.fields.permits);
      if (record.fields.structure) setField('structure', record.fields.structure);
      if (record.fields.lot) setField('lot', record.fields.lot);
      if (record.fields.coordinates !== undefined) setField('coordinates', record.fields.coordinates);
      if (record.fields.tax) setField('tax', record.fields.tax);

      setField('provenance', newProvenance);
      setField('derived_signals', mergedSignals);
      setField('content_hash', contentHash);
      setField('updated_at', new Date().toISOString());

      if (updates.length > 0) {
        values.push(parcelId);
        await pool.query(
          `UPDATE properties SET ${updates.join(', ')} WHERE parcel_id = $${paramIdx}`,
          values,
        );
        updatedCount++;
      }
    }
  }

  return { new_count: newCount, updated_count: updatedCount, removed_count: 0 };
}

// ---------------------------------------------------------------------------
// Main ingestion function
// ---------------------------------------------------------------------------

export interface IngestionOptions {
  county: string;
  limit?: number;
  runId: string;
}

/**
 * Run the full ingestion pipeline for a county.
 * Creates/uses parcel IDs, iterates all 8 source adapters, transforms,
 * loads into DB, and updates the pipeline_run record with results.
 *
 * The caller is responsible for creating the pipeline_run record beforehand
 * (with status 'running'). This function updates it on completion.
 */
export async function runIngestion(options: IngestionOptions): Promise<void> {
  const { county, limit, runId } = options;
  const startTime = Date.now();
  const realData = useRealData();
  const activeEntries = getSourceEntries();

  console.info('='.repeat(60));
  console.info(`Oracle Pipeline — County Ingestion`);
  console.info(`  County:    ${county}`);
  console.info(`  Limit:     ${limit ?? 'full'}`);
  console.info(`  Run ID:    ${runId}`);
  console.info(`  Mode:      ${limit ? 'pilot' : 'full county'}`);
  console.info(`  Data Mode: ${realData ? 'REAL (COJ ArcGIS)' : 'MOCK (generated)'}`);
  console.info('='.repeat(60));

  // Step 1: Run migrations
  console.info('\n[1/4] Running database migrations...');
  await runMigrations();

  const pool = getPool();

  // Step 2: Ensure data_sources rows exist (FK requirement for run_sources)
  console.info('\n[2/5] Ensuring data_sources catalog...');
  for (const entry of activeEntries) {
    await pool.query(
      `INSERT INTO data_sources (source_id, name, category, url, collection_method)
       VALUES ($1, $2, $3::source_category, $4, $5::collection_method)
       ON CONFLICT (source_id) DO NOTHING`,
      [entry.sourceId, entry.name, entry.category, entry.url, entry.collectionMethod],
    );
  }

  // Step 3: Get parcel IDs from seed data or real FDOT data
  console.info('\n[3/5] Loading parcel IDs...');
  let parcelIds: string[];
  let realFdotRecords: RawRecord[] | null = null;

  const targetCount = limit ?? 200;

  if (realData) {
    // REAL DATA MODE: Try pre-fetched files first, then live FDOT API
    const preFetched = loadPreFetchedFdotData();
    if (preFetched && preFetched.length > 0) {
      realFdotRecords = preFetched.slice(0, targetCount);
      parcelIds = realFdotRecords.map((r) => r.parcel_id);
      console.info(`  Using ${parcelIds.length} pre-fetched REAL parcels from data/real/`);
    } else {
      console.info(`  Fetching ${targetCount} real parcels from FDOT statewide parcel service (fallback)...`);
      realFdotRecords = await fetchDuvalParcels(targetCount);
      // Re-tag with coj source_id for consistency with the transform
      realFdotRecords = realFdotRecords.map((r) => ({ ...r, source_id: 'coj-duval-parcels' }));
      parcelIds = realFdotRecords.map((r) => r.parcel_id);
      console.info(`  Fetched ${parcelIds.length} REAL parcel IDs (FDOT fallback)`);
    }
  } else {
    // MOCK MODE: Get from DB or generate fabricated IDs
    const result = await pool.query<{ parcel_id: string }>(
      `SELECT parcel_id FROM properties WHERE county_jurisdiction = $1 ORDER BY parcel_id`,
      [county],
    );
    parcelIds = result.rows.map((r) => r.parcel_id);

    if (parcelIds.length < targetCount) {
      const existingSet = new Set(parcelIds);
      const needed = targetCount - parcelIds.length;
      console.info(`  Existing: ${parcelIds.length}, generating ${needed} additional test parcels...`);
      let generated = 0;
      let nextId = parcelIds.length + 1;
      while (generated < needed) {
        const id = `RE${String(nextId).padStart(7, '0')}`;
        if (!existingSet.has(id)) {
          parcelIds.push(id);
          generated++;
        }
        nextId++;
      }
    } else if (limit) {
      parcelIds = parcelIds.slice(0, limit);
    }
  }

  console.info(`  Found ${parcelIds.length} parcels to process`);

  // Step 3: Ingest each source
  console.info('\n[4/8] Ingesting sources...');
  const totalDelta: DeltaCounts = { new_count: 0, updated_count: 0, removed_count: 0 };
  const limitations: string[] = [];
  let hasFailure = false;
  const sourceCoverageResults: Array<{
    source_id: string;
    ingested_count: number;
    expected_count: number;
    status: 'success' | 'failed';
  }> = [];

  for (const entry of activeEntries) {
    const sourceStart = Date.now();
    console.info(`\n  --- Source: ${entry.sourceId} ---`);

    try {
      // In real-data mode, use pre-fetched FDOT records for the FDOT source;
      // other sources still use mock generators (until their real adapters are wired)
      let rawRecords: RawRecord[];
      if (realData && entry.sourceId === 'coj-duval-parcels' && realFdotRecords) {
        rawRecords = realFdotRecords;
        console.info(`    Using ${rawRecords.length} REAL records from COJ`);
      } else {
        rawRecords = parcelIds.map((id) => entry.mockGenerator(id));
      }
      console.info(`    Fetched ${rawRecords.length} raw records`);

      // Transform
      const transformed = entry.transform(rawRecords);
      console.info(`    Transformed ${transformed.length} records`);

      // Load into DB using feeder for backpressure
      const feederResult = await feed(
        transformed,
        async (batch) => {
          const delta = await loadRecordsDirect(runId, entry.sourceId, batch);
          return [delta];
        },
        { batchSize: 50, concurrency: 1, delayBetweenBatchesMs: 10 },
      );

      // Aggregate deltas
      const sourceDelta: DeltaCounts = { new_count: 0, updated_count: 0, removed_count: 0 };
      for (const d of feederResult.results) {
        sourceDelta.new_count += d.new_count;
        sourceDelta.updated_count += d.updated_count;
        sourceDelta.removed_count += d.removed_count;
      }

      totalDelta.new_count += sourceDelta.new_count;
      totalDelta.updated_count += sourceDelta.updated_count;
      totalDelta.removed_count += sourceDelta.removed_count;

      const sourceDuration = Date.now() - sourceStart;
      console.info(
        `    Result: +${sourceDelta.new_count} new, ~${sourceDelta.updated_count} updated (${sourceDuration}ms)`,
      );

      sourceCoverageResults.push({
        source_id: entry.sourceId,
        ingested_count: sourceDelta.new_count + sourceDelta.updated_count,
        expected_count: parcelIds.length,
        status: 'success',
      });

      // Record run_sources
      const ingestedCount = sourceDelta.new_count + sourceDelta.updated_count;
      await pool.query(
        `INSERT INTO run_sources (run_id, source_id, records_ingested, duration_ms, status, limitations)
         VALUES ($1, $2, $3, $4, 'success', $5)
         ON CONFLICT (run_id, source_id) DO UPDATE SET
           records_ingested = EXCLUDED.records_ingested,
           duration_ms = EXCLUDED.duration_ms,
           status = EXCLUDED.status`,
        [runId, entry.sourceId, ingestedCount, sourceDuration, null],
      );

      // Update data_sources with latest stats (feeds dashboard)
      await pool.query(
        `UPDATE data_sources SET last_successful_run = NOW(), record_count = $2 WHERE source_id = $1`,
        [entry.sourceId, ingestedCount],
      );
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      console.error(`    FAILED: ${error}`);
      hasFailure = true;
      limitations.push(`${entry.sourceId}: ${error}`);

      sourceCoverageResults.push({
        source_id: entry.sourceId,
        ingested_count: 0,
        expected_count: parcelIds.length,
        status: 'failed',
      });

      await pool.query(
        `INSERT INTO run_sources (run_id, source_id, records_ingested, duration_ms, status, limitations)
         VALUES ($1, $2, 0, 0, 'failed', $3)
         ON CONFLICT (run_id, source_id) DO UPDATE SET status = 'failed', limitations = EXCLUDED.limitations`,
        [runId, entry.sourceId, error],
      );
    }
  }

  // Step 5: Publish query table Parquet to Filebase (non-fatal)
  // NOTE: Parquet is published BEFORE index.json so its CID can be embedded in the index.
  // This enables CRM discovery: IPNS -> index.json -> query_table_cid -> Parquet file.
  console.info('\n[5/8] Publishing query table Parquet...');
  let queryTableCid: string | null = null;

  try {
    const allPropsForParquet = await pool.query<PropertyRecord>(
      `SELECT uuid, parcel_id, address, county_jurisdiction,
              assessed_value, market_value, ownership, current_owner,
              permits, structure, lot, coordinates, tax,
              provenance, derived_signals
       FROM properties
       WHERE county_jurisdiction = $1
       ORDER BY parcel_id`,
      [county],
    );

    const propsForParquet: PropertyRecord[] = allPropsForParquet.rows.map((row) => ({
      ...row,
      address: typeof row.address === 'string' ? JSON.parse(row.address) : row.address,
      ownership: typeof row.ownership === 'string' ? JSON.parse(row.ownership) : row.ownership,
      current_owner: typeof row.current_owner === 'string' ? JSON.parse(row.current_owner) : row.current_owner,
      permits: typeof row.permits === 'string' ? JSON.parse(row.permits) : row.permits,
      structure: typeof row.structure === 'string' ? JSON.parse(row.structure) : row.structure,
      lot: typeof row.lot === 'string' ? JSON.parse(row.lot) : row.lot,
      coordinates: typeof row.coordinates === 'string' ? JSON.parse(row.coordinates) : row.coordinates,
      tax: typeof row.tax === 'string' ? JSON.parse(row.tax) : row.tax,
      provenance: typeof row.provenance === 'string' ? JSON.parse(row.provenance) : row.provenance,
      derived_signals: typeof row.derived_signals === 'string' ? JSON.parse(row.derived_signals) : row.derived_signals,
    }));

    if (propsForParquet.length > 0) {
      const flatRows = propsForParquet.map(flattenProperty);

      // Query table validation gate — no duplicates, no null parcel_ids
      const distinctCount = new Set(flatRows.map((r) => r.parcel_id)).size;
      const nullCount = flatRows.filter((r) => !r.parcel_id).length;

      if (flatRows.length !== distinctCount || nullCount > 0) {
        console.error(
          `  Query table gate FAILED: ${flatRows.length} rows, ${distinctCount} distinct, ${nullCount} nulls — skipping publish`,
        );
      } else {
        console.info(
          `  Query table gate PASSED: ${flatRows.length} rows, all unique, 0 nulls`,
        );

        const parquetBuffer = await buildParquetBuffer(flatRows);

        const bktParquet = filebaseBucket();
        const parquetKey = `${KEY_PREFIX.queryTable}${county}/query-table.parquet`;
        await uploadParquet(bktParquet, parquetKey, parquetBuffer);

        queryTableCid = await getCid(bktParquet, parquetKey);
        console.info(`  Query table published: ${propsForParquet.length} rows, cid=${queryTableCid}`);
      }
    } else {
      console.info('  No properties for query table, skipping');
    }
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    console.error(`  Query table Parquet publish failed (non-fatal): ${error}`);
  }

  // Step 6: Publish index.json to Filebase and update IPNS (non-fatal)
  // index.json now includes query_table_cid so CRM can resolve: IPNS -> index -> Parquet CID.
  console.info('\n[6/8] Publishing index.json to Filebase...');
  let publishedArtifactCid: string | null = null;
  let ipnsPointer: string | null = null;

  try {
    const bkt = filebaseBucket();
    const allProps = await pool.query<PropertyRecord>(
      `SELECT uuid, parcel_id, address, county_jurisdiction,
              assessed_value, market_value, ownership, current_owner,
              permits, structure, lot, coordinates, tax,
              provenance, derived_signals, content_hash,
              created_at, updated_at
       FROM properties
       WHERE county_jurisdiction = $1
       ORDER BY parcel_id`,
      [county],
    );

    const properties: PropertyRecord[] = allProps.rows.map((row) => ({
      ...row,
      address: typeof row.address === 'string' ? JSON.parse(row.address) : row.address,
      ownership: typeof row.ownership === 'string' ? JSON.parse(row.ownership) : row.ownership,
      current_owner: typeof row.current_owner === 'string' ? JSON.parse(row.current_owner) : row.current_owner,
      permits: typeof row.permits === 'string' ? JSON.parse(row.permits) : row.permits,
      structure: typeof row.structure === 'string' ? JSON.parse(row.structure) : row.structure,
      lot: typeof row.lot === 'string' ? JSON.parse(row.lot) : row.lot,
      coordinates: typeof row.coordinates === 'string' ? JSON.parse(row.coordinates) : row.coordinates,
      tax: typeof row.tax === 'string' ? JSON.parse(row.tax) : row.tax,
      provenance: typeof row.provenance === 'string' ? JSON.parse(row.provenance) : row.provenance,
      derived_signals: typeof row.derived_signals === 'string' ? JSON.parse(row.derived_signals) : row.derived_signals,
    }));

    if (properties.length > 0) {
      // Per-property JSON publish SKIPPED — hits Filebase free-tier 500 pin limit.
      // The query-table Parquet (step 5) is the canonical queryable artifact.
      console.info(`  Skipping per-property JSON publish (${properties.length} properties — exceeds pin limit)`);

      // Upload index.json — includes Parquet CID for CRM discovery
      const indexKey = `${KEY_PREFIX.openData}index.json`;
      const indexData: Record<string, unknown> = {
        county,
        property_count: properties.length,
        published_at: new Date().toISOString(),
        run_id: runId,
      };

      // Embed Parquet CID so CRM can discover the query table via IPNS -> index.json
      if (queryTableCid) {
        indexData.query_table_cid = queryTableCid;
        indexData.query_table_url = `https://ipfs.filebase.io/ipfs/${queryTableCid}`;
      }

      await uploadJson(bkt, indexKey, indexData);

      // Get CID from Filebase
      publishedArtifactCid = await getCid(bkt, indexKey);

      // Update IPNS pointer
      if (publishedArtifactCid) {
        const ipnsResult = await upsertName(IPNS_LABEL, publishedArtifactCid);
        ipnsPointer = ipnsResult.network_key;
        console.info(`  Published: cid=${publishedArtifactCid}, ipns=${ipnsPointer}`);
      } else {
        console.warn('  Published files but could not retrieve CID from Filebase');
      }
    } else {
      console.info('  No properties to publish, skipping');
    }
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    console.error(`  Publish to Filebase failed (non-fatal): ${error}`);
  }

  // Step 7: Publish dataset-coverage.json (oracle convention, non-fatal)
  console.info('\n[7/8] Publishing dataset-coverage.json...');
  try {
    const bktCoverage = filebaseBucket();
    const recordCount = await pool.query<{ count: string }>(
      'SELECT COUNT(*) as count FROM properties WHERE county_jurisdiction = $1',
      [county],
    );
    const coverageTotalRecords = parseInt(recordCount.rows[0]?.count ?? '0', 10);

    const coverage = {
      county,
      updated_at: new Date().toISOString(),
      run_id: runId,
      sources: sourceCoverageResults.map((s) => ({
        source_id: s.source_id,
        ingested_count: s.ingested_count,
        expected_count: s.expected_count,
        status: s.status,
      })),
      total_properties: coverageTotalRecords,
      ipns_pointer: ipnsPointer,
      artifact_cid: publishedArtifactCid,
      query_table_cid: queryTableCid,
    };

    const coverageKey = `${KEY_PREFIX.datasetCoverage}${county}/dataset-coverage.json`;
    await uploadJson(bktCoverage, coverageKey, coverage);
    console.info(`  Dataset coverage published: ${coverageKey}`);
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    console.error(`  Dataset coverage publish failed (non-fatal): ${error}`);
  }

  // Step 8: Update pipeline_run
  console.info('\n[8/8] Finalizing pipeline run...');
  const recordCount = await pool.query<{ count: string }>(
    'SELECT COUNT(*) as count FROM properties WHERE county_jurisdiction = $1',
    [county],
  );
  const totalRecords = parseInt(recordCount.rows[0]?.count ?? '0', 10);
  const status = hasFailure ? 'partial' : 'success';

  await pool.query(
    `UPDATE pipeline_runs
     SET completed_at = NOW(), status = $2, record_count = $3,
         delta_new = $4, delta_updated = $5, delta_removed = $6,
         source_limitations = $7,
         published_artifact_cid = $8,
         ipns_pointer = $9,
         query_table_cid = $10
     WHERE run_id = $1`,
    [runId, status, totalRecords, totalDelta.new_count, totalDelta.updated_count, totalDelta.removed_count, JSON.stringify(limitations), publishedArtifactCid, ipnsPointer, queryTableCid],
  );

  const totalDuration = Date.now() - startTime;

  console.info('\n' + '='.repeat(60));
  console.info('INGESTION COMPLETE');
  console.info('='.repeat(60));
  console.info(`  Status:        ${status}`);
  console.info(`  Run ID:        ${runId}`);
  console.info(`  Total Records: ${totalRecords}`);
  console.info(`  New:           ${totalDelta.new_count}`);
  console.info(`  Updated:       ${totalDelta.updated_count}`);
  console.info(`  Removed:       ${totalDelta.removed_count}`);
  console.info(`  Published CID: ${publishedArtifactCid ?? 'none'}`);
  console.info(`  Query Table:   ${queryTableCid ?? 'none'}`);
  console.info(`  IPNS Pointer:  ${ipnsPointer ?? 'none'}`);
  console.info(`  Duration:      ${totalDuration}ms`);
  if (limitations.length > 0) {
    console.info(`  Limitations:   ${limitations.length}`);
    for (const lim of limitations) {
      console.info(`    - ${lim}`);
    }
  }
  console.info('='.repeat(60));
}
