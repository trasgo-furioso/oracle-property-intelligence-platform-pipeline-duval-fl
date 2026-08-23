/**
 * Query API routes for property search and single-property detail.
 * T053 — GET /api/properties/search, GET /api/properties/:parcel_id
 *
 * Reads published IPFS Parquet data via DuckDB instead of Postgres.
 * The Parquet table has flat columns (no JSONB), e.g. roof_age_years, is_waterfront.
 */

import { Hono } from 'hono';
import { queryAll, exec, loadHttpfs, queryOne as duckQueryOne } from '../lib/duckdb.js';
import { queryOne } from '../lib/db.js';
import { getCid, bucket as filebaseBucket, KEY_PREFIX } from '../lib/filebase.js';

const queryRoutes = new Hono();

// ---------------------------------------------------------------------------
// DuckDB initialization
// ---------------------------------------------------------------------------

const VIEW_NAME = 'properties';

let duckdbReady = false;
let duckdbInitPromise: Promise<void> | null = null;

/**
 * Initialize DuckDB with httpfs and create a view over the published Parquet.
 * Gets the Parquet CID from Filebase and uses a direct IPFS gateway URL.
 */
async function ensureDuckDb(): Promise<void> {
  if (duckdbReady) return;
  if (duckdbInitPromise) return duckdbInitPromise;

  duckdbInitPromise = (async () => {
    try {
      await loadHttpfs();

      // Get the Parquet file CID directly from Filebase
      const bkt = filebaseBucket();
      const parquetKey = `${KEY_PREFIX.queryTable}duval/query-table.parquet`;
      const parquetCid = await getCid(bkt, parquetKey);

      if (!parquetCid) {
        throw new Error('Query table Parquet not found in Filebase — run a pipeline ingestion first');
      }

      const url = `https://ipfs.filebase.io/ipfs/${parquetCid}`;
      // Materialize into a table (not a view) so DuckDB fetches the Parquet once
      await exec(`DROP TABLE IF EXISTS ${VIEW_NAME};`);
      await exec(`CREATE TABLE ${VIEW_NAME} AS SELECT * FROM read_parquet('${url}');`);
      duckdbReady = true;
      console.info(`[query-routes] DuckDB initialized with Parquet CID: ${parquetCid}`);
    } catch (err) {
      duckdbInitPromise = null;
      throw err;
    }
  })();

  return duckdbInitPromise;
}

/**
 * Re-initialize the DuckDB view (e.g., after a new pipeline run publishes fresh data).
 */
async function refreshDuckDbView(): Promise<void> {
  duckdbReady = false;
  duckdbInitPromise = null;
  await ensureDuckDb();
}

// ---------------------------------------------------------------------------
// Query type definitions (flat Parquet columns — no JSONB)
// ---------------------------------------------------------------------------

type QueryType =
  | 'roof_age_gt_15'
  | 'water_view'
  | 'ownership_tenure_gt_10'
  | 'regional_owners'
  | 'transit_walking'
  | 'starbucks_walking';

interface QueryDefinition {
  label: string;
  where: string;
  signalColumn: string;
  signalLabel: string;
}

const QUERY_DEFINITIONS: Record<QueryType, QueryDefinition> = {
  roof_age_gt_15: {
    label: 'Roofs older than 15 years',
    where: 'roof_age_years > 15',
    signalColumn: 'roof_age_years',
    signalLabel: 'Roof Age (yrs)',
  },
  water_view: {
    label: 'View of water',
    where: 'is_waterfront = true',
    signalColumn: 'water_proximity_ft',
    signalLabel: 'Water Distance (ft)',
  },
  ownership_tenure_gt_10: {
    label: 'No ownership change in 10+ years',
    where: 'ownership_tenure_years > 10',
    signalColumn: 'ownership_tenure_years',
    signalLabel: 'Tenure (yrs)',
  },
  regional_owners: {
    label: 'Regional owners',
    where: 'is_regional_owner = true',
    signalColumn: 'current_owner_name',
    signalLabel: 'Owner Location',
  },
  transit_walking: {
    label: 'Walking distance to public transit',
    where: 'within_walking_transit = true',
    signalColumn: 'transit_distance_mi',
    signalLabel: 'Transit (mi)',
  },
  starbucks_walking: {
    label: 'Walking distance to Starbucks',
    where: 'within_walking_starbucks = true',
    signalColumn: 'starbucks_distance_mi',
    signalLabel: 'Starbucks (mi)',
  },
};

// ---------------------------------------------------------------------------
// GET /api/properties/search?query=<type>&page=<n>&limit=<n>
// ---------------------------------------------------------------------------

queryRoutes.get('/api/properties/search', async (c) => {
  const queryType = c.req.query('query') as QueryType | undefined;

  if (!queryType || !QUERY_DEFINITIONS[queryType]) {
    return c.json(
      {
        error: 'Invalid query type',
        valid_types: Object.keys(QUERY_DEFINITIONS),
      },
      400,
    );
  }

  const def = QUERY_DEFINITIONS[queryType];
  const page = Math.max(1, parseInt(c.req.query('page') ?? '1', 10));
  const limit = Math.min(100, Math.max(1, parseInt(c.req.query('limit') ?? '50', 10)));
  const offset = (page - 1) * limit;

  try {
    await ensureDuckDb();

    // Count total matching rows
    const countResult = await duckQueryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${VIEW_NAME} WHERE ${def.where}`,
    );
    const total = Number(countResult?.count ?? 0);

    // ---------------------------------------------------------------------------
    // Edge-6: Compute data gaps — count properties excluded due to missing
    // coordinate or proximity data relevant to the query type.
    // ---------------------------------------------------------------------------
    const DATA_GAP_QUERIES: Partial<Record<QueryType, string>> = {
      roof_age_gt_15: `SELECT COUNT(*) as gap_count FROM ${VIEW_NAME} WHERE roof_age_years IS NULL`,
      water_view: `SELECT COUNT(*) as gap_count FROM ${VIEW_NAME} WHERE water_proximity_ft IS NULL`,
      ownership_tenure_gt_10: `SELECT COUNT(*) as gap_count FROM ${VIEW_NAME} WHERE ownership_tenure_years IS NULL`,
      transit_walking: `SELECT COUNT(*) as gap_count FROM ${VIEW_NAME} WHERE transit_distance_mi IS NULL`,
      starbucks_walking: `SELECT COUNT(*) as gap_count FROM ${VIEW_NAME} WHERE starbucks_distance_mi IS NULL`,
    };

    let dataGaps: { excluded_count: number; reason: string } | undefined;
    const gapQuery = DATA_GAP_QUERIES[queryType];
    if (gapQuery) {
      const gapResult = await duckQueryOne<{ gap_count: number }>(gapQuery);
      const gapCount = Number(gapResult?.gap_count ?? 0);
      if (gapCount > 0) {
        const reasonMap: Record<string, string> = {
          roof_age_gt_15: 'missing year_built data (FDOT source requires authentication token)',
          water_view: 'missing coordinate/water proximity data',
          ownership_tenure_gt_10: 'missing sale date data',
          transit_walking: 'missing coordinate/transit proximity data',
          starbucks_walking: 'missing coordinate/Starbucks proximity data',
        };
        dataGaps = {
          excluded_count: gapCount,
          reason: `${gapCount} properties excluded due to ${reasonMap[queryType] ?? 'missing data'}`,
        };
      }
    }

    // Fetch results
    const rows = await queryAll<Record<string, unknown>>(
      `SELECT
        parcel_id,
        full_address as address,
        assessed_value,
        ${def.signalColumn} as signal_value,
        source_count,
        reconciliation_confidence,
        provenance_last_run,
        roof_age_years,
        ownership_tenure_years,
        is_regional_owner,
        water_proximity_ft,
        is_waterfront,
        transit_distance_mi,
        starbucks_distance_mi,
        within_walking_transit,
        within_walking_starbucks
      FROM ${VIEW_NAME}
      WHERE ${def.where}
      ORDER BY parcel_id
      LIMIT ${limit} OFFSET ${offset}`,
    );

    return c.json({
      query_type: queryType,
      query_label: def.label,
      signal_label: def.signalLabel,
      total,
      page,
      limit,
      pages: Math.ceil(total / limit),
      results: rows,
      ...(dataGaps ? { data_gaps: dataGaps } : {}),
    });
  } catch (err) {
    console.error('[query-routes] search error:', err);
    return c.json({ error: 'Search query failed', detail: String(err) }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/properties/search/types — list available query types
// ---------------------------------------------------------------------------

queryRoutes.get('/api/properties/search/types', (c) => {
  const types = Object.entries(QUERY_DEFINITIONS).map(([key, def]) => ({
    type: key,
    label: def.label,
    signal_label: def.signalLabel,
  }));
  return c.json({ types });
});

// ---------------------------------------------------------------------------
// GET /api/properties/:parcel_id — single property detail (from Parquet)
// ---------------------------------------------------------------------------

queryRoutes.get('/api/properties/:parcel_id', async (c) => {
  const parcelId = c.req.param('parcel_id');

  try {
    await ensureDuckDb();

    const row = await duckQueryOne<Record<string, unknown>>(
      `SELECT * FROM ${VIEW_NAME} WHERE parcel_id = '${parcelId.replace(/'/g, "''")}'`,
    );

    if (!row) {
      return c.json({ error: 'Property not found' }, 404);
    }

    return c.json({ property: row });
  } catch (err) {
    console.error('[query-routes] detail error:', err);
    return c.json({ error: 'Property lookup failed', detail: String(err) }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/properties/refresh — force re-read of Parquet view
// ---------------------------------------------------------------------------

queryRoutes.post('/api/properties/refresh', async (c) => {
  try {
    await refreshDuckDbView();
    return c.json({ status: 'ok', message: 'DuckDB view refreshed' });
  } catch (err) {
    console.error('[query-routes] refresh error:', err);
    return c.json({ error: 'Refresh failed', detail: String(err) }, 500);
  }
});

export { queryRoutes };
