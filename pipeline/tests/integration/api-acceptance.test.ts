/**
 * API Acceptance Tests — exercises user-story acceptance criteria
 * directly against the deployed API (headless HTTP calls, no browser).
 *
 * Run: npm run test:api
 */

import { describe, test, expect } from 'vitest';

const API = 'https://d5sfa8vgu8mcx.cloudfront.net';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface RunSummary {
  run_id: string;
  county: string;
  started_at: string;
  completed_at: string | null;
  status: string;
  record_count: number;
  delta_new: number;
  delta_updated: number;
  delta_removed: number;
  published_artifact_cid: string | null;
  ipns_pointer: string | null;
}

interface RunDetail extends RunSummary {
  sources: {
    source_id: string;
    source_name: string;
    records_ingested: number;
    duration_ms: number;
    status: string;
  }[];
}

interface Stats {
  totalProperties: number;
  lastRun: RunSummary | null;
  ipnsStatus: string;
  ipnsPointer: string;
  artifactCid: string;
  sourceCount: number;
  healthySources: number;
}

async function triggerRun(limit: number): Promise<{ run_id: string }> {
  const res = await fetch(`${API}/api/runs/trigger`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ county: 'duval', limit }),
  });
  expect(res.ok).toBe(true);
  return res.json() as Promise<{ run_id: string }>;
}

async function pollRunComplete(timeoutMs = 120_000, pollIntervalMs = 5_000): Promise<RunSummary> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const res = await fetch(`${API}/api/runs`);
    const body = (await res.json()) as { runs: RunSummary[] };
    const latest = body.runs[0];
    if (latest && (latest.status === 'success' || latest.status === 'failed')) {
      return latest;
    }
    await new Promise((r) => setTimeout(r, pollIntervalMs));
  }
  throw new Error(`Run did not complete within ${timeoutMs / 1000}s`);
}

async function getStats(): Promise<Stats> {
  const res = await fetch(`${API}/api/stats`);
  expect(res.ok).toBe(true);
  return res.json() as Promise<Stats>;
}

async function getRunDetail(runId: string): Promise<RunDetail> {
  const res = await fetch(`${API}/api/runs/${runId}`);
  expect(res.ok).toBe(true);
  return res.json() as Promise<RunDetail>;
}

// ---------------------------------------------------------------------------
// Shared state between sequential tests
// ---------------------------------------------------------------------------

let runId1: string;
let runId2: string;
let countBefore: number;
let countAfter: number;

// ---------------------------------------------------------------------------
// US1 — Continuous Incremental Ingestion
// ---------------------------------------------------------------------------

describe('US1 — Continuous Incremental Ingestion', () => {
  test(
    'Test 1: Trigger a run and verify it completes successfully',
    async () => {
      const { run_id } = await triggerRun(50);
      expect(run_id).toBeTruthy();
      runId1 = run_id;

      const completed = await pollRunComplete();
      expect(completed.status).toBe('success');
      expect(completed.record_count).toBeGreaterThan(0);
      expect(completed.delta_updated).toBeGreaterThanOrEqual(0);
    },
    { timeout: 120_000 },
  );

  test(
    'Test 2: Progressive property growth across runs (CRITICAL)',
    async () => {
      const statsBefore = await getStats();
      countBefore = statsBefore.totalProperties;

      const { run_id } = await triggerRun(250);
      runId2 = run_id;

      const completed = await pollRunComplete();
      expect(completed.status).toBe('success');

      const statsAfter = await getStats();
      countAfter = statsAfter.totalProperties;

      // The pipeline was at 200 — triggering with limit=250 should add 50 new
      expect(countAfter).toBeGreaterThan(countBefore);
      expect(completed.delta_new).toBeGreaterThan(0);

      console.log(
        `Progressive growth: ${countBefore} -> ${countAfter} (+${countAfter - countBefore})`,
      );
    },
    { timeout: 120_000 },
  );

  test(
    'Test 3: Idempotent re-run (same limit produces no new properties)',
    async () => {
      const { run_id } = await triggerRun(250);
      expect(run_id).toBeTruthy();

      const completed = await pollRunComplete();
      expect(completed.status).toBe('success');
      expect(completed.delta_new).toBe(0);
      expect(completed.delta_updated).toBeGreaterThan(0);
      // record_count should match what was seen after test 2
      expect(completed.record_count).toBe(countAfter);
    },
    { timeout: 120_000 },
  );

  test(
    'Test 4: Verify provenance on search results',
    async () => {
      const res = await fetch(
        `${API}/api/properties/search?query=roof_age_gt_15&limit=5`,
      );
      expect(res.ok).toBe(true);
      const body = (await res.json()) as {
        results: Record<string, unknown>[];
      };

      expect(body.results.length).toBeGreaterThan(0);
      for (const prop of body.results) {
        // Provenance is expressed as flat fields: source_count + reconciliation_confidence
        expect(prop.source_count).toBeDefined();
        expect(Number(prop.source_count)).toBeGreaterThanOrEqual(1);
        expect(prop.reconciliation_confidence).toBeDefined();
        expect(prop.provenance_last_run).toBeDefined();
      }
    },
    { timeout: 30_000 },
  );
});

// ---------------------------------------------------------------------------
// US2 — IPFS Publishing + IPNS
// ---------------------------------------------------------------------------

describe('US2 — IPFS Publishing + IPNS', () => {
  test(
    'Test 5: Run publishes to IPFS with valid CID and IPNS pointer',
    async () => {
      // Use run from test 2 (progressive growth run)
      const id = runId2 ?? (await getStats()).lastRun?.run_id;
      expect(id).toBeTruthy();

      const detail = await getRunDetail(id!);
      expect(detail.published_artifact_cid).toBeTruthy();
      expect(detail.published_artifact_cid!.startsWith('Qm')).toBe(true);
      expect(detail.ipns_pointer).toBeTruthy();
      expect(detail.ipns_pointer!.startsWith('k51')).toBe(true);
    },
    { timeout: 30_000 },
  );

  test(
    'Test 6: IPNS resolves to valid artifact',
    async () => {
      const ipnsUrl =
        'https://ipfs.filebase.io/ipns/k51qzi5uqu5dggq0h9xylfc0kr0kpw7i4zcacnfrymz9sjv7mpeze4femaujcz';
      const res = await fetch(ipnsUrl);
      expect(res.ok).toBe(true);

      const body = (await res.json()) as {
        county?: string;
        property_count?: number;
      };
      expect(body.county).toBe('duval');
      expect(body.property_count).toBeGreaterThan(0);
    },
    { timeout: 30_000 },
  );
});

// ---------------------------------------------------------------------------
// US3 — Pipeline Run History
// ---------------------------------------------------------------------------

describe('US3 — Pipeline Run History', () => {
  test(
    'Test 7: Run history shows all runs',
    async () => {
      const res = await fetch(`${API}/api/runs`);
      expect(res.ok).toBe(true);
      const body = (await res.json()) as { runs: RunSummary[] };

      expect(body.runs.length).toBeGreaterThanOrEqual(2);
      for (const run of body.runs) {
        expect(run.started_at).toBeTruthy();
        expect(run.status).toBeTruthy();
        expect(typeof run.delta_new).toBe('number');
        expect(typeof run.delta_updated).toBe('number');
      }
    },
    { timeout: 30_000 },
  );

  test(
    'Test 8: Source details available per run',
    async () => {
      const runsRes = await fetch(`${API}/api/runs`);
      const { runs } = (await runsRes.json()) as { runs: RunSummary[] };
      const latestId = runs[0]!.run_id;

      const detail = await getRunDetail(latestId);
      expect(detail.sources).toBeDefined();
      expect(detail.sources.length).toBe(8);

      for (const src of detail.sources) {
        expect(src.source_id).toBeTruthy();
        expect(typeof src.records_ingested).toBe('number');
        expect(src.status).toBeTruthy();
      }
    },
    { timeout: 30_000 },
  );

  test(
    'Test 9: Dashboard stats reflect real data',
    async () => {
      const stats = await getStats();
      expect(stats.totalProperties).toBeGreaterThanOrEqual(250);
      expect(stats.sourceCount).toBe(8);
      expect(stats.lastRun).not.toBeNull();
    },
    { timeout: 30_000 },
  );
});

// ---------------------------------------------------------------------------
// US4 — Property Intelligence Queries
// ---------------------------------------------------------------------------

describe('US4 — Property Intelligence Queries', () => {
  const QUERY_TYPES = [
    'roof_age_gt_15',
    'water_view',
    'ownership_tenure_gt_10',
    'regional_owners',
    'transit_walking',
    'starbucks_walking',
  ] as const;

  test(
    'Test 10: All 6 query types return valid responses',
    async () => {
      for (const qt of QUERY_TYPES) {
        const res = await fetch(`${API}/api/properties/search?query=${qt}`);
        expect(res.status).toBe(200);

        const body = (await res.json()) as { results: unknown[] };
        expect(Array.isArray(body.results)).toBe(true);
      }
    },
    { timeout: 60_000 },
  );

  test(
    'Test 11: Single property detail',
    async () => {
      // Get a parcel_id from search
      const searchRes = await fetch(
        `${API}/api/properties/search?query=roof_age_gt_15&limit=1`,
      );
      const { results } = (await searchRes.json()) as {
        results: { parcel_id: string }[];
      };
      expect(results.length).toBeGreaterThan(0);

      const parcelId = results[0]!.parcel_id;
      const detailRes = await fetch(`${API}/api/properties/${parcelId}`);
      expect(detailRes.ok).toBe(true);

      const { property } = (await detailRes.json()) as {
        property: Record<string, unknown>;
      };
      expect(property.parcel_id).toBe(parcelId);
      expect(property.full_address ?? property.address).toBeTruthy();
      expect(property.source_count).toBeDefined();
      expect(property.reconciliation_confidence).toBeDefined();
    },
    { timeout: 30_000 },
  );
});

// ---------------------------------------------------------------------------
// US4 — Agent Chat
// ---------------------------------------------------------------------------

describe('US4 — Agent Chat', () => {
  test(
    'Test 12: Agent health',
    async () => {
      const res = await fetch(`${API}/api/agent/health`);
      expect(res.ok).toBe(true);

      const body = (await res.json()) as {
        status: string;
        model: string;
        tools: string[];
      };
      expect(body.status).toBe('ok');
      expect(body.model).toBeTruthy();
      expect(body.tools.length).toBe(3);
    },
    { timeout: 15_000 },
  );

  test(
    'Test 13: Agent answers a question',
    async () => {
      const res = await fetch(`${API}/api/agent/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: [
            {
              role: 'user',
              content: 'How many properties are in the database?',
            },
          ],
        }),
      });
      expect(res.ok).toBe(true);

      // Vercel AI SDK data stream — collect full response text
      const text = await res.text();
      expect(text.length).toBeGreaterThan(0);

      // The response should contain some numeric content (the property count)
      const hasNumber = /\d+/.test(text);
      expect(hasNumber).toBe(true);
    },
    { timeout: 60_000 },
  );
});
