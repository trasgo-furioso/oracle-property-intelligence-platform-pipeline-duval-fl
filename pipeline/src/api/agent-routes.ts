/**
 * Agent API routes — POST /api/agent/chat (streaming), GET /api/agent/health.
 * T058 — Wires Vercel AI SDK agent to Hono endpoints.
 *
 * Property tools query published IPFS Parquet via DuckDB httpfs (MCP-backed),
 * while operational tools (run history) remain on Postgres.
 */

import { Hono } from 'hono';
import { streamText, type CoreMessage } from 'ai';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createOpenAI } from '@ai-sdk/openai';
import { tool } from 'ai';
import { z } from 'zod';
import { query as pgQuery } from '../lib/db.js';
import {
  queryAll as duckQueryAll,
  loadHttpfs,
  exec as duckExec,
} from '../lib/duckdb.js';
import { getCid, bucket as filebaseBucket, KEY_PREFIX } from '../lib/filebase.js';

const agentRoutes = new Hono();

// ---------------------------------------------------------------------------
// DuckDB httpfs initialization (MCP-backed data layer)
// ---------------------------------------------------------------------------

let duckHttpfsReady = false;
let duckViewReady = false;

const VIEW_NAME = 'properties';

/**
 * Ensure DuckDB httpfs is loaded and the Parquet table is created.
 * Uses direct CID resolution from Filebase (same approach as query-routes)
 * instead of IPNS sub-path which 404s.
 */
async function ensureDuckView(): Promise<void> {
  if (!duckHttpfsReady) {
    await loadHttpfs();
    duckHttpfsReady = true;
  }

  if (!duckViewReady) {
    const bkt = filebaseBucket();
    const parquetKey = `${KEY_PREFIX.queryTable}duval/query-table.parquet`;
    const parquetCid = await getCid(bkt, parquetKey);

    if (!parquetCid) {
      throw new Error('Query table Parquet not found in Filebase — run a pipeline ingestion first');
    }

    const url = `https://ipfs.filebase.io/ipfs/${parquetCid}`;
    await duckExec(`DROP TABLE IF EXISTS ${VIEW_NAME};`);
    await duckExec(`CREATE TABLE ${VIEW_NAME} AS SELECT * FROM read_parquet('${url}');`);
    duckViewReady = true;
    console.info(`[agent-routes] DuckDB table "${VIEW_NAME}" created → CID ${parquetCid.slice(0, 16)}…`);
  }
}

// ---------------------------------------------------------------------------
// System prompt (inline to avoid cross-workspace import)
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `You are a Duval County, Florida property intelligence assistant. You help users explore and analyze property data from the Oracle Pipeline.

CAPABILITIES:
- Query the published property dataset using SQL (DuckDB over IPFS-published Parquet)
- Look up individual properties by parcel ID
- Answer questions about roof ages, ownership tenure, water proximity, transit access, and more

DATA SOURCE:
All property data is read from published IPFS artifacts via DuckDB httpfs — the same
data layer used by the MCP server. No live database dependency for property queries.

PIPELINE RUN STATUSES (pipeline_runs table):
Valid values for the status column: 'running', 'success', 'partial', 'failed'
Do NOT use 'completed' or any other value — it will cause a database error.

RULES:
1. Always use the available tools to answer data questions - never make up property data
2. Always cite source provenance when presenting results
3. When users ask broad questions, use appropriate filters and LIMIT clauses
4. Present results in a clear, organized format
5. If a query returns no results, explain possible reasons
6. For multi-criteria queries, combine conditions in a single query
7. Always mention data freshness (last pipeline run)
8. Use DuckDB-compatible SQL (not PostgreSQL). Use dot notation for nested fields.

AVAILABLE COLUMNS on the "properties" table (flat Parquet schema, no nested JSON):
- parcel_id (VARCHAR), uuid (VARCHAR)
- full_address (VARCHAR), street (VARCHAR), address_city (VARCHAR), state (VARCHAR), address_zip (VARCHAR)
- county_jurisdiction (VARCHAR)
- assessed_value (DOUBLE), market_value (DOUBLE)
- current_owner_name (VARCHAR), current_owner_type (VARCHAR)
- year_built (INT32), sqft (INT32), stories (INT32), bedrooms (INT32), bathrooms (INT32)
- roof_type (VARCHAR), construction_type (VARCHAR), use_code (VARCHAR), use_description (VARCHAR)
- lot_area_sqft (DOUBLE), lot_area_acres (DOUBLE), zoning (VARCHAR)
- lat (DOUBLE), lng (DOUBLE)
- taxable_value (DOUBLE), tax_year (INT32), annual_tax (DOUBLE)
- roof_age_years (INT32), ownership_tenure_years (INT32)
- is_regional_owner (BOOLEAN), water_proximity_ft (DOUBLE), is_waterfront (BOOLEAN)
- transit_distance_mi (DOUBLE), starbucks_distance_mi (DOUBLE)
- within_walking_transit (BOOLEAN), within_walking_starbucks (BOOLEAN)
- source_count (INT32), reconciliation_confidence (DOUBLE), provenance_last_run (VARCHAR)
- provenance_sources (VARCHAR), provenance_timestamps (VARCHAR)`;

// ---------------------------------------------------------------------------
// Model provider
// ---------------------------------------------------------------------------

function getModel() {
  const modelId = process.env.AI_MODEL ?? 'claude-sonnet-4-20250514';

  if (modelId.startsWith('gpt-') || modelId.startsWith('o1') || modelId.startsWith('o3')) {
    const openai = createOpenAI({ apiKey: process.env.OPENAI_API_KEY });
    return openai(modelId);
  }

  const anthropic = createAnthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  return anthropic(modelId);
}

// ---------------------------------------------------------------------------
// Tools (MCP-backed: DuckDB httpfs over published IPFS Parquet)
// ---------------------------------------------------------------------------

const queryPropertiesTool = tool({
  description:
    'Query Duval County properties from published IPFS Parquet data via DuckDB. ' +
    'The table is "properties" with flat columns (no nested JSON). ' +
    'Key columns: parcel_id, full_address, assessed_value, market_value, roof_age_years, ' +
    'ownership_tenure_years, is_regional_owner, water_proximity_ft, is_waterfront, ' +
    'transit_distance_mi, starbucks_distance_mi, within_walking_transit, ' +
    'within_walking_starbucks, current_owner_name, year_built, sqft, ' +
    'stories, bedrooms, bathrooms, roof_type, source_count, reconciliation_confidence. ' +
    'Always LIMIT results to 20 unless counting.',
  parameters: z.object({
    sql: z.string().describe('DuckDB SQL query to execute against the properties table'),
    explanation: z.string().describe('Brief explanation of what this query does'),
  }),
  execute: async ({ sql, explanation }) => {
    try {
      const trimmed = sql.trim().toUpperCase();
      if (!trimmed.startsWith('SELECT')) {
        return { error: 'Only SELECT queries are allowed', explanation };
      }

      await ensureDuckView();
      const raw = await duckQueryAll(sql);
      // DuckDB returns BigInt for aggregate functions — convert to Number for JSON
      const rows = raw.map((row: Record<string, unknown>) => {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(row)) {
          out[k] = typeof v === 'bigint' ? Number(v) : v;
        }
        return out;
      });
      return {
        results: rows.slice(0, 100),
        row_count: rows.length,
        query_executed: sql,
        explanation,
        data_source: 'Published IPFS Parquet via DuckDB httpfs (MCP-backed)',
      };
    } catch (err) {
      return { error: `Query failed: ${String(err)}`, query_attempted: sql, explanation };
    }
  },
});

const getPropertyDetailTool = tool({
  description: 'Look up a single property by parcel ID from published IPFS data.',
  parameters: z.object({
    parcel_id: z.string().describe('The parcel ID to look up'),
  }),
  execute: async ({ parcel_id }) => {
    try {
      await ensureDuckView();
      const escapedId = parcel_id.replace(/'/g, "''");
      const rawRows = await duckQueryAll(
        `SELECT * FROM ${VIEW_NAME} WHERE parcel_id = '${escapedId}' LIMIT 1`,
      );
      if (rawRows.length === 0) return { error: `No property found: ${parcel_id}` };
      // Convert BigInt values for JSON serialization
      const prop: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(rawRows[0] as Record<string, unknown>)) {
        prop[k] = typeof v === 'bigint' ? Number(v) : v;
      }
      return {
        property: prop,
        data_source: 'Published IPFS Parquet via DuckDB httpfs (MCP-backed)',
      };
    } catch (err) {
      return { error: `Lookup failed: ${String(err)}`, parcel_id };
    }
  },
});

const getRunHistoryTool = tool({
  description: 'Get recent pipeline run history. Shows run status, record counts, and deltas.',
  parameters: z.object({
    limit: z.number().optional().default(5).describe('Number of recent runs to retrieve'),
  }),
  execute: async ({ limit }) => {
    try {
      const result = await pgQuery(
        `SELECT run_id, county, started_at, completed_at, status, record_count,
                delta_new, delta_updated, delta_removed, published_artifact_cid, ipns_pointer
         FROM pipeline_runs
         ORDER BY started_at DESC
         LIMIT $1`,
        [limit],
      );
      return { runs: result.rows, count: result.rowCount };
    } catch (err) {
      return { error: `Failed to fetch run history: ${String(err)}` };
    }
  },
});

const agentTools = {
  queryProperties: queryPropertiesTool,
  getPropertyDetail: getPropertyDetailTool,
  getRunHistory: getRunHistoryTool,
};

// ---------------------------------------------------------------------------
// POST /api/agent/chat — streaming response
// ---------------------------------------------------------------------------

agentRoutes.post('/api/agent/chat', async (c) => {
  try {
    const body = await c.req.json<{ messages: CoreMessage[] }>();

    if (!body.messages || !Array.isArray(body.messages)) {
      return c.json({ error: 'messages array is required' }, 400);
    }

    const result = streamText({
      model: getModel(),
      system: SYSTEM_PROMPT,
      messages: body.messages,
      tools: agentTools,
      maxSteps: 5,
    });

    return result.toDataStreamResponse();
  } catch (err) {
    console.error('[agent-routes] chat error:', err);
    return c.json({ error: 'Agent chat failed', detail: String(err) }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/agent/health
// ---------------------------------------------------------------------------

agentRoutes.get('/api/agent/health', (c) => {
  const hasApiKey = !!(process.env.ANTHROPIC_API_KEY || process.env.OPENAI_API_KEY);
  const model = process.env.AI_MODEL ?? 'claude-sonnet-4-20250514';

  return c.json({
    status: hasApiKey ? 'ok' : 'no_api_key',
    model,
    tools: Object.keys(agentTools),
    data_layer: 'MCP-backed (DuckDB httpfs over published IPFS Parquet)',
    duckdb_view_ready: duckViewReady,
    timestamp: new Date().toISOString(),
  });
});

export { agentRoutes };
