/**
 * Pipeline API routes — T047
 * GET /api/runs, GET /api/runs/:id, GET /api/sources, GET /api/stats, POST /api/runs/trigger
 */

import { Hono } from 'hono';
import { query, queryOne } from '../lib/db.js';
import { runIngestion } from '../lib/ingest.js';
import type {
  PipelineRun,
  PipelineRunStatus,
  DataSource,
  RunSource,
} from '../lib/types.js';

// ---------------------------------------------------------------------------
// Types for API responses
// ---------------------------------------------------------------------------

interface StatsResponse {
  totalProperties: number;
  lastRun: {
    run_id: string;
    started_at: string;
    status: PipelineRunStatus;
    delta_new: number;
    delta_updated: number;
    delta_removed: number;
  } | null;
  ipnsStatus: 'live' | 'stale' | 'pending';
  ipnsPointer: string | null;
  artifactCid: string | null;
  queryTableCid: string | null;
  queryTableUrl: string | null;
  sourceCount: number;
  healthySources: number;
}

interface RunListItem {
  run_id: string;
  county: string;
  started_at: string;
  completed_at: string | null;
  status: PipelineRunStatus;
  record_count: number;
  delta_new: number;
  delta_updated: number;
  delta_removed: number;
  source_limitations: string[];
  published_artifact_cid: string | null;
  ipns_pointer: string | null;
  query_table_cid: string | null;
  query_table_url: string | null;
}

interface RunDetail extends RunListItem {
  sources: Array<{
    source_id: string;
    source_name: string;
    records_ingested: number;
    duration_ms: number | null;
    status: string;
    limitations: string | null;
  }>;
}

interface SourceListItem {
  source_id: string;
  name: string;
  category: string;
  url: string;
  collection_method: string;
  last_successful_run: string | null;
  record_count: number;
  limitations: string | null;
  status: 'healthy' | 'slow' | 'stale' | 'error';
}

// ---------------------------------------------------------------------------
// Route definitions
// ---------------------------------------------------------------------------

export function createApiRoutes(): Hono {
  const api = new Hono();

  // ── GET /api/stats ───────────────────────────────────────────────────────
  api.get('/api/stats', async (c) => {
    try {
      // Total properties
      const propCount = await queryOne<{ count: string }>(
        'SELECT COUNT(*)::text as count FROM properties',
      );

      // Last run
      const lastRun = await queryOne<{
        run_id: string;
        started_at: Date;
        status: PipelineRunStatus;
        delta_new: number;
        delta_updated: number;
        delta_removed: number;
        published_artifact_cid: string | null;
        ipns_pointer: string | null;
        query_table_cid: string | null;
      }>(
        `SELECT run_id, started_at, status, delta_new, delta_updated, delta_removed,
                published_artifact_cid, ipns_pointer, query_table_cid
         FROM pipeline_runs ORDER BY started_at DESC LIMIT 1`,
      );

      // Source count
      const sourceCount = await queryOne<{ total: string; healthy: string }>(
        `SELECT COUNT(*)::text as total,
                COUNT(*) FILTER (WHERE last_successful_run > NOW() - INTERVAL '24 hours')::text as healthy
         FROM data_sources`,
      );

      // Determine IPNS status
      let ipnsStatus: 'live' | 'stale' | 'pending' = 'pending';
      if (lastRun?.ipns_pointer) {
        const hoursSinceRun =
          (Date.now() - new Date(lastRun.started_at).getTime()) / (1000 * 60 * 60);
        ipnsStatus = hoursSinceRun < 24 ? 'live' : 'stale';
      }

      const stats: StatsResponse = {
        totalProperties: parseInt(propCount?.count ?? '0', 10),
        lastRun: lastRun
          ? {
              run_id: lastRun.run_id,
              started_at: new Date(lastRun.started_at).toISOString(),
              status: lastRun.status,
              delta_new: lastRun.delta_new,
              delta_updated: lastRun.delta_updated,
              delta_removed: lastRun.delta_removed,
            }
          : null,
        ipnsStatus,
        ipnsPointer: lastRun?.ipns_pointer ?? null,
        artifactCid: lastRun?.published_artifact_cid ?? null,
        queryTableCid: lastRun?.query_table_cid ?? null,
        queryTableUrl: lastRun?.query_table_cid
          ? `https://ipfs.filebase.io/ipfs/${lastRun.query_table_cid}`
          : null,
        sourceCount: parseInt(sourceCount?.total ?? '0', 10),
        healthySources: parseInt(sourceCount?.healthy ?? '0', 10),
      };

      return c.json(stats);
    } catch (err) {
      // If DB is unreachable, return defaults
      console.error('[api/stats] error:', err);
      return c.json({
        totalProperties: 0,
        lastRun: null,
        ipnsStatus: 'pending',
        ipnsPointer: null,
        artifactCid: null,
        queryTableCid: null,
        queryTableUrl: null,
        sourceCount: 0,
        healthySources: 0,
      } satisfies StatsResponse);
    }
  });

  // ── GET /api/runs ────────────────────────────────────────────────────────
  api.get('/api/runs', async (c) => {
    try {
      const page = parseInt(c.req.query('page') ?? '1', 10);
      const limit = Math.min(parseInt(c.req.query('limit') ?? '20', 10), 100);
      const offset = (page - 1) * limit;

      const totalResult = await queryOne<{ count: string }>(
        'SELECT COUNT(*)::text as count FROM pipeline_runs',
      );
      const total = parseInt(totalResult?.count ?? '0', 10);

      const { rows } = await query<{
        run_id: string;
        county: string;
        started_at: Date;
        completed_at: Date | null;
        status: PipelineRunStatus;
        record_count: number;
        delta_new: number;
        delta_updated: number;
        delta_removed: number;
        source_limitations: string[] | string;
        published_artifact_cid: string | null;
        ipns_pointer: string | null;
        query_table_cid: string | null;
      }>(
        `SELECT run_id, county, started_at, completed_at, status,
                record_count, delta_new, delta_updated, delta_removed,
                source_limitations, published_artifact_cid, ipns_pointer,
                query_table_cid
         FROM pipeline_runs
         ORDER BY started_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset],
      );

      const runs: RunListItem[] = rows.map((r) => ({
        run_id: r.run_id,
        county: r.county,
        started_at: new Date(r.started_at).toISOString(),
        completed_at: r.completed_at ? new Date(r.completed_at).toISOString() : null,
        status: r.status,
        record_count: r.record_count,
        delta_new: r.delta_new,
        delta_updated: r.delta_updated,
        delta_removed: r.delta_removed,
        source_limitations: Array.isArray(r.source_limitations)
          ? r.source_limitations
          : typeof r.source_limitations === 'string'
            ? JSON.parse(r.source_limitations)
            : [],
        published_artifact_cid: r.published_artifact_cid,
        ipns_pointer: r.ipns_pointer,
        query_table_cid: r.query_table_cid,
        query_table_url: r.query_table_cid
          ? `https://ipfs.filebase.io/ipfs/${r.query_table_cid}`
          : null,
      }));

      return c.json({ runs, total, page, limit });
    } catch (err) {
      console.error('[api/runs] error:', err);
      return c.json({ runs: [], total: 0, page: 1, limit: 20 });
    }
  });

  // ── GET /api/runs/:id ────────────────────────────────────────────────────
  api.get('/api/runs/:id', async (c) => {
    try {
      const runId = c.req.param('id');

      const run = await queryOne<{
        run_id: string;
        county: string;
        started_at: Date;
        completed_at: Date | null;
        status: PipelineRunStatus;
        record_count: number;
        delta_new: number;
        delta_updated: number;
        delta_removed: number;
        source_limitations: string[] | string;
        published_artifact_cid: string | null;
        ipns_pointer: string | null;
        query_table_cid: string | null;
      }>(
        `SELECT run_id, county, started_at, completed_at, status,
                record_count, delta_new, delta_updated, delta_removed,
                source_limitations, published_artifact_cid, ipns_pointer,
                query_table_cid
         FROM pipeline_runs WHERE run_id = $1`,
        [runId],
      );

      if (!run) {
        return c.json({ error: 'Run not found' }, 404);
      }

      // Get run sources with source names
      const { rows: sources } = await query<{
        source_id: string;
        source_name: string;
        records_ingested: number;
        duration_ms: number | null;
        status: string;
        limitations: string | null;
      }>(
        `SELECT rs.source_id, ds.name as source_name,
                rs.records_ingested, rs.duration_ms, rs.status, rs.limitations
         FROM run_sources rs
         LEFT JOIN data_sources ds ON rs.source_id = ds.source_id
         WHERE rs.run_id = $1
         ORDER BY ds.name`,
        [runId],
      );

      const detail: RunDetail = {
        run_id: run.run_id,
        county: run.county,
        started_at: new Date(run.started_at).toISOString(),
        completed_at: run.completed_at ? new Date(run.completed_at).toISOString() : null,
        status: run.status,
        record_count: run.record_count,
        delta_new: run.delta_new,
        delta_updated: run.delta_updated,
        delta_removed: run.delta_removed,
        source_limitations: Array.isArray(run.source_limitations)
          ? run.source_limitations
          : typeof run.source_limitations === 'string'
            ? JSON.parse(run.source_limitations)
            : [],
        published_artifact_cid: run.published_artifact_cid,
        ipns_pointer: run.ipns_pointer,
        query_table_cid: run.query_table_cid,
        query_table_url: run.query_table_cid
          ? `https://ipfs.filebase.io/ipfs/${run.query_table_cid}`
          : null,
        sources: sources.map((s) => ({
          source_id: s.source_id,
          source_name: s.source_name ?? s.source_id,
          records_ingested: s.records_ingested,
          duration_ms: s.duration_ms,
          status: s.status,
          limitations: s.limitations,
        })),
      };

      return c.json(detail);
    } catch (err) {
      console.error('[api/runs/:id] error:', err);
      return c.json({ error: 'Internal server error' }, 500);
    }
  });

  // ── GET /api/sources ─────────────────────────────────────────────────────
  api.get('/api/sources', async (c) => {
    try {
      const { rows } = await query<{
        source_id: string;
        name: string;
        category: string;
        url: string;
        collection_method: string;
        last_successful_run: Date | null;
        record_count: number;
        limitations: string | null;
      }>(
        `SELECT source_id, name, category, url, collection_method,
                last_successful_run, record_count, limitations
         FROM data_sources
         ORDER BY name`,
      );

      const now = Date.now();
      const sources: SourceListItem[] = rows.map((s) => {
        let status: SourceListItem['status'] = 'healthy';
        if (!s.last_successful_run) {
          status = 'stale';
        } else {
          const hoursSince = (now - new Date(s.last_successful_run).getTime()) / (1000 * 60 * 60);
          if (hoursSince > 48) status = 'error';
          else if (hoursSince > 24) status = 'stale';
        }
        if (s.limitations?.toLowerCase().includes('slow')) {
          status = 'slow';
        }

        return {
          source_id: s.source_id,
          name: s.name,
          category: s.category,
          url: s.url,
          collection_method: s.collection_method,
          last_successful_run: s.last_successful_run
            ? new Date(s.last_successful_run).toISOString()
            : null,
          record_count: s.record_count,
          limitations: s.limitations,
          status,
        };
      });

      return c.json({ sources });
    } catch (err) {
      console.error('[api/sources] error:', err);
      return c.json({ sources: [] });
    }
  });

  // ── POST /api/runs/trigger ───────────────────────────────────────────────
  api.post('/api/runs/trigger', async (c) => {
    try {
      const body = await c.req.json().catch(() => ({}));
      const county = (body as { county?: string }).county ?? 'duval';
      const limit = (body as { limit?: number }).limit ?? 400000;

      // Create a new pipeline run record
      const runId = crypto.randomUUID();
      await query(
        `INSERT INTO pipeline_runs (run_id, county, started_at, status, record_count, delta_new, delta_updated, delta_removed, source_limitations)
         VALUES ($1, $2, NOW(), 'running', 0, 0, 0, 0, '[]'::jsonb)`,
        [runId, county],
      );

      // Fire-and-forget: run ingestion in background
      runIngestion({ county, limit, runId }).catch(async (err) => {
        console.error('[trigger] ingestion failed:', err);
        await query(
          `UPDATE pipeline_runs SET status = 'failed', completed_at = NOW(), source_limitations = $2 WHERE run_id = $1`,
          [runId, JSON.stringify([String(err)])],
        );
      });

      return c.json(
        {
          run_id: runId,
          county,
          status: 'running',
          message: 'Pipeline run triggered',
        },
        201,
      );
    } catch (err) {
      console.error('[api/runs/trigger] error:', err);
      return c.json({ error: 'Failed to trigger pipeline run' }, 500);
    }
  });

  return api;
}
