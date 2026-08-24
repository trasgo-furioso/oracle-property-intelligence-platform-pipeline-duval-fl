/**
 * Fetch real Duval County property data from multiple public sources.
 * Designed to run on an EC2 instance with a US IP (avoids geo-blocking).
 *
 * Sources:
 *   A. FDOT ArcGIS — statewide parcels filtered to Duval (CO_NO=16)
 *   B. COJ CityBiz ArcGIS — Duval County's own parcel service
 *   C. Overpass API — Starbucks locations in Jacksonville area
 *   D. JTA GTFS — transit stop coordinates
 *
 * Usage:
 *   npx tsx pipeline/src/scripts/fetch-real-data.ts [--limit=2000]
 *
 * Output:
 *   pipeline/data/real/fdot-parcels.json
 *   pipeline/data/real/coj-parcels.json
 *   pipeline/data/real/starbucks.json
 *   pipeline/data/real/transit-stops.json
 *   pipeline/data/real/summary.json
 *   pipeline/data/seeds/duval-real.csv
 */

import { writeFileSync, mkdirSync, existsSync, createWriteStream } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const REAL_DIR = resolve(__dirname, '..', '..', 'data', 'real');
const SEEDS_DIR = resolve(__dirname, '..', '..', 'data', 'seeds');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url: string, label: string, timeoutMs = 60_000): Promise<Response> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText}`);
      }
      return response;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  [${label}] Attempt ${attempt}/${MAX_RETRIES} failed: ${msg}`);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      } else {
        throw new Error(`[${label}] All ${MAX_RETRIES} attempts failed. Last error: ${msg}`);
      }
    }
  }
  throw new Error('unreachable');
}

// ---------------------------------------------------------------------------
// A. FDOT ArcGIS — Statewide Parcels (Duval = CO_NO 16)
// ---------------------------------------------------------------------------

interface ArcGISFeature {
  attributes: Record<string, unknown>;
  geometry?: { rings?: number[][][]; x?: number; y?: number };
}

interface ArcGISResponse {
  features?: ArcGISFeature[];
  exceededTransferLimit?: boolean;
  error?: { code: number; message: string };
}

function computeCentroid(rings: number[][][]): { lat: number; lng: number } | null {
  if (!rings?.[0]?.length) return null;
  const outer = rings[0];
  let sx = 0, sy = 0, n = 0;
  for (const pt of outer) {
    if (pt && pt.length >= 2) { sx += pt[0]!; sy += pt[1]!; n++; }
  }
  if (n === 0) return null;
  return { lng: sx / n, lat: sy / n };
}

async function fetchFdotParcels(limit: number): Promise<{
  records: Array<Record<string, unknown>>;
  count: number;
  error?: string;
}> {
  console.info('\n--- Source A: FDOT ArcGIS (statewide, CO_NO=16) ---');

  // FeatureServer/16 is the Duval County layer — no CO_NO filter needed
  const BASE = 'https://gis.fdot.gov/arcgis/rest/services/Parcels/FeatureServer/16';
  const OUT_FIELDS = 'PARCELNO,CO_NO,APTS_STRT,APTS_CITY,APTS_STATE,APTS_ZIP,OWN_NAME,OWN_ADDR1,OWN_CITY,OWN_STATE,OWN_ZIPCD,DOR_UC,JV,AV_NSD,TV_NSD,LND_VAL,NCONST_VAL,ACT_YR_BLT,EFF_YR_BLT,TOT_LVG_AR,NO_BULDNG,NO_RES_UNTS,ACREAGE,S_LEGAL,SALE_PRC,SALE_DT';
  const PAGE_SIZE = 1000;
  const records: Array<Record<string, unknown>> = [];
  let offset = 0;
  let hasMore = true;

  try {
    while (hasMore && records.length < limit) {
      const batchSize = Math.min(limit - records.length, PAGE_SIZE);
      const params = new URLSearchParams({
        where: '1=1',
        outFields: OUT_FIELDS,
        returnGeometry: 'true',
        f: 'json',
        resultOffset: String(offset),
        resultRecordCount: String(batchSize),
        outSR: '4326',
      });

      const url = `${BASE}/query?${params}`;
      const resp = await fetchWithRetry(url, 'FDOT');
      const data = (await resp.json()) as ArcGISResponse;

      if (data.error) {
        console.error(`  FDOT API error: ${data.error.message}`);
        break;
      }

      const features = data.features ?? [];
      if (features.length === 0) { hasMore = false; break; }

      for (const f of features) {
        if (records.length >= limit) break;
        const a = f.attributes;
        const parcelNo = String(a.PARCELNO ?? '').trim();
        if (!parcelNo) continue;

        const centroid = f.geometry?.rings ? computeCentroid(f.geometry.rings) : null;

        records.push({
          parcel_id: parcelNo,
          source: 'fdot',
          address_street: String(a.APTS_STRT ?? '').trim() || null,
          address_city: String(a.APTS_CITY ?? '').trim() || null,
          address_state: String(a.APTS_STATE ?? '').trim() || 'FL',
          address_zip: String(a.APTS_ZIP ?? '').trim() || null,
          owner_name: String(a.OWN_NAME ?? '').trim() || null,
          owner_address: String(a.OWN_ADDR1 ?? '').trim() || null,
          owner_city: String(a.OWN_CITY ?? '').trim() || null,
          owner_state: String(a.OWN_STATE ?? '').trim() || null,
          owner_zip: String(a.OWN_ZIPCD ?? '').trim() || null,
          dor_use_code: String(a.DOR_UC ?? '').trim() || null,
          just_value: a.JV ?? null,
          assessed_value: a.AV_NSD ?? null,
          taxable_value: a.TV_NSD ?? null,
          land_value: a.LND_VAL ?? null,
          building_value: a.NCONST_VAL ?? null,
          year_built: a.ACT_YR_BLT ?? null,
          effective_year_built: a.EFF_YR_BLT ?? null,
          total_living_area: a.TOT_LVG_AR ?? null,
          num_buildings: a.NO_BULDNG ?? null,
          num_res_units: a.NO_RES_UNTS ?? null,
          acreage: a.ACREAGE ?? null,
          short_legal: String(a.S_LEGAL ?? '').trim() || null,
          sale_price: a.SALE_PRC ?? null,
          sale_date: a.SALE_DT ?? null,
          lat: centroid?.lat ?? null,
          lng: centroid?.lng ?? null,
        });
      }

      offset += features.length;
      hasMore = data.exceededTransferLimit === true && records.length < limit;

      console.info(`  Fetched ${records.length}/${limit} parcels (offset=${offset})...`);

      if (hasMore) await sleep(300);
    }

    console.info(`  FDOT total: ${records.length} parcels fetched`);
    return { records, count: records.length };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  FDOT FAILED: ${msg}`);
    return { records, count: records.length, error: msg };
  }
}

// ---------------------------------------------------------------------------
// B. COJ CityBiz ArcGIS (Duval County's own service)
// ---------------------------------------------------------------------------

async function fetchCojParcels(limit: number): Promise<{
  records: Array<Record<string, unknown>>;
  count: number;
  error?: string;
}> {
  console.info('\n--- Source B: COJ CityBiz ArcGIS ---');

  const BASE = 'https://maps.coj.net/coj/rest/services/CityBiz/Parcels/MapServer/0';
  const PAGE_SIZE = 1000;
  const records: Array<Record<string, unknown>> = [];
  let offset = 0;
  let hasMore = true;

  try {
    while (hasMore && records.length < limit) {
      const batchSize = Math.min(limit - records.length, PAGE_SIZE);
      const params = new URLSearchParams({
        where: '1=1',
        outFields: '*',
        returnGeometry: 'true',
        f: 'json',
        resultOffset: String(offset),
        resultRecordCount: String(batchSize),
        outSR: '4326',
      });

      const url = `${BASE}/query?${params}`;
      const resp = await fetchWithRetry(url, 'COJ', 60_000);
      const data = (await resp.json()) as ArcGISResponse;

      if (data.error) {
        console.error(`  COJ API error: ${data.error.message}`);
        break;
      }

      const features = data.features ?? [];
      if (features.length === 0) { hasMore = false; break; }

      for (const f of features) {
        if (records.length >= limit) break;
        const a = f.attributes;
        const reNo = String(a.RE_NO ?? a.PARCEL_ID ?? a.RE ?? '').trim();
        if (!reNo) continue;

        const centroid = f.geometry?.rings
          ? computeCentroid(f.geometry.rings)
          : f.geometry?.x != null
            ? { lng: f.geometry.x, lat: f.geometry.y ?? 0 }
            : null;

        records.push({
          parcel_id: reNo,
          source: 'coj',
          ...Object.fromEntries(
            Object.entries(a).map(([k, v]) => [k.toLowerCase(), v]),
          ),
          lat: centroid?.lat ?? null,
          lng: centroid?.lng ?? null,
        });
      }

      offset += features.length;
      hasMore = data.exceededTransferLimit === true && records.length < limit;

      console.info(`  COJ fetched ${records.length}/${limit} parcels...`);

      if (hasMore) await sleep(300);
    }

    console.info(`  COJ total: ${records.length} parcels fetched`);
    return { records, count: records.length };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  COJ FAILED: ${msg}`);
    return { records, count: records.length, error: msg };
  }
}

// ---------------------------------------------------------------------------
// C. Overpass API — Starbucks in Jacksonville area
// ---------------------------------------------------------------------------

interface OverpassElement {
  type: string;
  id: number;
  lat?: number;
  lon?: number;
  tags?: Record<string, string>;
}

async function fetchStarbucks(): Promise<{
  locations: Array<{ name: string; lat: number; lng: number; address?: string }>;
  count: number;
  error?: string;
}> {
  console.info('\n--- Source C: Overpass API (Starbucks) ---');

  // Jacksonville bounding box: roughly 30.1,-82.0 to 30.6,-81.3
  const query = '[out:json];node["brand"="Starbucks"](30.1,-82.0,30.6,-81.3);out;';
  const url = `https://overpass-api.de/api/interpreter?data=${encodeURIComponent(query)}`;

  try {
    const resp = await fetchWithRetry(url, 'Overpass', 90_000);
    const data = (await resp.json()) as { elements?: OverpassElement[] };

    const elements = data.elements ?? [];
    const locations = elements
      .filter((e): e is OverpassElement & { lat: number; lon: number } =>
        e.lat != null && e.lon != null)
      .map((e) => ({
        name: e.tags?.name ?? 'Starbucks',
        lat: e.lat,
        lng: e.lon,
        address: [e.tags?.['addr:housenumber'], e.tags?.['addr:street']].filter(Boolean).join(' ') || undefined,
      }));

    console.info(`  Starbucks locations found: ${locations.length}`);
    return { locations, count: locations.length };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  Overpass FAILED: ${msg}`);
    return { locations: [], count: 0, error: msg };
  }
}

// ---------------------------------------------------------------------------
// D. JTA GTFS — Transit stops
// ---------------------------------------------------------------------------

async function fetchTransitStops(): Promise<{
  stops: Array<{ stop_id: string; stop_name: string; lat: number; lng: number }>;
  count: number;
  error?: string;
}> {
  console.info('\n--- Source D: JTA GTFS (transit stops) ---');

  const GTFS_URL = 'https://www.jtafla.com/media/gtfs/google_transit.zip';
  const tmpDir = '/tmp/gtfs';

  try {
    // Download the GTFS zip
    console.info('  Downloading GTFS zip...');
    const resp = await fetchWithRetry(GTFS_URL, 'GTFS', 120_000);
    const buffer = Buffer.from(await resp.arrayBuffer());

    // Write to /tmp and extract
    mkdirSync(tmpDir, { recursive: true });
    const zipPath = `${tmpDir}/google_transit.zip`;
    writeFileSync(zipPath, buffer);

    // Extract stops.txt using unzip
    try {
      execSync(`unzip -o "${zipPath}" stops.txt -d "${tmpDir}" 2>/dev/null`, { timeout: 10_000 });
    } catch {
      // Try with jar if unzip not available
      execSync(`cd "${tmpDir}" && jar xf google_transit.zip stops.txt 2>/dev/null`, { timeout: 10_000 });
    }

    const stopsPath = `${tmpDir}/stops.txt`;
    if (!existsSync(stopsPath)) {
      throw new Error('stops.txt not found in GTFS archive');
    }

    // Parse stops.txt (CSV)
    const stopsContent = require('node:fs').readFileSync(stopsPath, 'utf-8') as string;
    const lines = stopsContent.split('\n').filter((l: string) => l.trim());
    const headerLine = lines[0]!;
    const headers = headerLine.split(',').map((h: string) => h.trim().toLowerCase());

    const stopIdIdx = headers.indexOf('stop_id');
    const stopNameIdx = headers.indexOf('stop_name');
    const latIdx = headers.indexOf('stop_lat');
    const lngIdx = headers.indexOf('stop_lon');

    if (latIdx < 0 || lngIdx < 0) {
      throw new Error(`stops.txt missing lat/lon columns. Headers: ${headers.join(',')}`);
    }

    const stops: Array<{ stop_id: string; stop_name: string; lat: number; lng: number }> = [];

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i]!.split(',');
      const lat = parseFloat(cols[latIdx] ?? '');
      const lng = parseFloat(cols[lngIdx] ?? '');
      if (isNaN(lat) || isNaN(lng)) continue;

      stops.push({
        stop_id: (cols[stopIdIdx] ?? '').replace(/"/g, '').trim(),
        stop_name: (cols[stopNameIdx] ?? '').replace(/"/g, '').trim(),
        lat,
        lng,
      });
    }

    console.info(`  Transit stops parsed: ${stops.length}`);
    return { stops, count: stops.length };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`  GTFS FAILED: ${msg}`);
    return { stops: [], count: 0, error: msg };
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2);
  const limitArg = args.find((a) => a.startsWith('--limit='));
  const limit = limitArg ? parseInt(limitArg.split('=')[1]!, 10) : 400000;

  console.info('='.repeat(70));
  console.info('  ORACLE PIPELINE — Real Duval County Data Fetcher');
  console.info(`  Parcel limit: ${limit}`);
  console.info(`  Timestamp:    ${new Date().toISOString()}`);
  console.info('='.repeat(70));

  // Ensure output directories
  mkdirSync(REAL_DIR, { recursive: true });
  mkdirSync(SEEDS_DIR, { recursive: true });

  // ---------- Fetch all sources ----------

  const fdot = await fetchFdotParcels(limit);
  const coj = await fetchCojParcels(limit); // COJ as supplementary (no cap)
  const starbucks = await fetchStarbucks();
  const transit = await fetchTransitStops();

  // ---------- Save results ----------
  // Stream JSON arrays and CSV files to disk one record at a time to avoid
  // the ~512 MB string-length limit of JSON.stringify on 374k+ records.

  console.info('\n--- Saving results ---');

  /**
   * Stream an array of objects to a file as a valid JSON array.
   * Each record is serialized individually to avoid hitting the ~512 MB
   * string-length limit of a single JSON.stringify() call on 374k+ records.
   */
  function writeJsonArrayStreaming(filePath: string, records: Record<string, unknown>[]): Promise<void> {
    return new Promise((resolvePromise, reject) => {
      const ws = createWriteStream(filePath, { encoding: 'utf-8' });
      ws.on('error', reject);
      ws.write('[\n');
      for (let i = 0; i < records.length; i++) {
        const line = JSON.stringify(records[i]!);
        ws.write(i < records.length - 1 ? line + ',\n' : line + '\n');
      }
      ws.end(']\n', () => resolvePromise());
    });
  }

  /** Stream seed CSV rows to a file. */
  function writeSeedCsv(filePath: string, records: Record<string, unknown>[]): Promise<void> {
    return new Promise((resolvePromise, reject) => {
      const ws = createWriteStream(filePath, { encoding: 'utf-8' });
      ws.on('error', reject);
      ws.write('parcel_id,address_street,address_city,address_state,address_zip\n');
      for (const r of records) {
        const street = String(r.address_street ?? '').replace(/"/g, '""');
        const city = String(r.address_city ?? '').replace(/"/g, '""');
        const state = String(r.address_state ?? 'FL');
        const zip = String(r.address_zip ?? '');
        ws.write(`${r.parcel_id},"${street}","${city}","${state}","${zip}"\n`);
      }
      ws.end(() => resolvePromise());
    });
  }

  if (fdot.records.length > 0) {
    await writeJsonArrayStreaming(resolve(REAL_DIR, 'fdot-parcels.json'), fdot.records);
    console.info(`  Saved fdot-parcels.json (${fdot.records.length} records)`);
  }

  if (coj.records.length > 0) {
    await writeJsonArrayStreaming(resolve(REAL_DIR, 'coj-parcels.json'), coj.records);
    console.info(`  Saved coj-parcels.json (${coj.records.length} records)`);
  }

  // Starbucks and transit are small — keep as regular JSON
  if (starbucks.locations.length > 0) {
    writeFileSync(resolve(REAL_DIR, 'starbucks.json'), JSON.stringify(starbucks.locations, null, 2));
    console.info(`  Saved starbucks.json (${starbucks.locations.length} locations)`);
  }

  if (transit.stops.length > 0) {
    writeFileSync(resolve(REAL_DIR, 'transit-stops.json'), JSON.stringify(transit.stops, null, 2));
    console.info(`  Saved transit-stops.json (${transit.stops.length} stops)`);
  }

  // ---------- Generate seed CSV from FDOT (primary source) ----------

  const seedRecords = fdot.records.length > 0 ? fdot.records : coj.records;
  if (seedRecords.length > 0) {
    const seedPath = resolve(SEEDS_DIR, 'duval-real.csv');
    await writeSeedCsv(seedPath, seedRecords);
    console.info(`  Saved duval-real.csv (${seedRecords.length} parcels)`);

    // Also overwrite duval.csv so the pipeline uses real data
    const mainSeedPath = resolve(SEEDS_DIR, 'duval.csv');
    await writeSeedCsv(mainSeedPath, seedRecords);
    console.info(`  Updated duval.csv with real parcel IDs`);
  }

  // ---------- Summary ----------

  const summary = {
    fetched_at: new Date().toISOString(),
    sources: {
      fdot: {
        count: fdot.count,
        status: fdot.error ? 'partial' : 'success',
        error: fdot.error ?? null,
      },
      coj: {
        count: coj.count,
        status: coj.error ? 'partial' : 'success',
        error: coj.error ?? null,
      },
      starbucks: {
        count: starbucks.count,
        status: starbucks.error ? 'failed' : 'success',
        error: starbucks.error ?? null,
      },
      transit: {
        count: transit.count,
        status: transit.error ? 'failed' : 'success',
        error: transit.error ?? null,
      },
    },
    sample_parcels: seedRecords.slice(0, 3),
  };

  writeFileSync(resolve(REAL_DIR, 'summary.json'), JSON.stringify(summary, null, 2));
  console.info(`  Saved summary.json`);

  // ---------- Print report ----------

  console.info('\n' + '='.repeat(70));
  console.info('  FETCH COMPLETE');
  console.info('='.repeat(70));
  console.info(`  FDOT parcels:     ${fdot.count}${fdot.error ? ` (error: ${fdot.error})` : ''}`);
  console.info(`  COJ parcels:      ${coj.count}${coj.error ? ` (error: ${coj.error})` : ''}`);
  console.info(`  Starbucks:        ${starbucks.count}${starbucks.error ? ` (error: ${starbucks.error})` : ''}`);
  console.info(`  Transit stops:    ${transit.count}${transit.error ? ` (error: ${transit.error})` : ''}`);
  console.info('');

  if (seedRecords.length > 0) {
    console.info('  Sample parcels:');
    for (const r of seedRecords.slice(0, 3)) {
      console.info(`    ${r.parcel_id} | ${r.address_street}, ${r.address_city} ${r.address_zip} | JV=$${r.just_value} | UC=${r.dor_use_code}`);
    }
  }

  console.info('='.repeat(70));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
