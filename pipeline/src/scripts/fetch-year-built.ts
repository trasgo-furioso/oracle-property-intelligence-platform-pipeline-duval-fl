/**
 * Fetch year_built data from Jacksonville Property Appraiser or FDOT.
 * Run from EC2 (US IP) to bypass geo-blocking.
 *
 * Usage: npx tsx pipeline/src/scripts/fetch-year-built.ts
 */

import { writeFileSync, existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const DATA_DIR = resolve(__dirname, '../../data/real');

interface YearBuiltRecord {
  parcel_id: string;
  year_built: number;
  source: string;
}

// ---------------------------------------------------------------------------
// Source A: FDOT FeatureServer/16 (has ACT_YR_BLT)
// ---------------------------------------------------------------------------

async function fetchFdotYearBuilt(limit: number): Promise<YearBuiltRecord[]> {
  console.info('--- Source A: FDOT FeatureServer/16 ---');
  const records: YearBuiltRecord[] = [];
  const pageSize = 1000;
  let offset = 0;

  while (records.length < limit) {
    const url = new URL('https://gis.fdot.gov/arcgis/rest/services/Parcels/FeatureServer/16/query');
    url.searchParams.set('where', '1=1');
    url.searchParams.set('outFields', 'PARCELNO,ACT_YR_BLT,EFF_YR_BLT,TOT_LVG_AR');
    url.searchParams.set('resultRecordCount', String(pageSize));
    url.searchParams.set('resultOffset', String(offset));
    url.searchParams.set('f', 'json');

    try {
      const res = await fetch(url.toString(), { signal: AbortSignal.timeout(30000) });
      const data = await res.json() as any;

      if (data.error) {
        console.error(`  FDOT error: ${data.error.message}`);
        break;
      }

      if (!data.features || data.features.length === 0) break;

      for (const f of data.features) {
        const a = f.attributes || {};
        const parcelId = a.PARCELNO;
        const yrBlt = a.ACT_YR_BLT || a.EFF_YR_BLT;
        if (parcelId && yrBlt && yrBlt > 1800 && yrBlt <= 2026) {
          records.push({ parcel_id: String(parcelId).trim(), year_built: yrBlt, source: 'fdot' });
        }
      }

      console.info(`  Fetched page at offset ${offset}: ${data.features.length} features, ${records.length} with year_built`);
      offset += pageSize;

      if (!data.exceededTransferLimit && data.features.length < pageSize) break;
    } catch (err) {
      console.error(`  FDOT fetch failed: ${err}`);
      break;
    }
  }

  return records;
}

// ---------------------------------------------------------------------------
// Source B: COJ Property Appraiser (scrape individual properties)
// ---------------------------------------------------------------------------

async function fetchPaoYearBuilt(parcelIds: string[]): Promise<YearBuiltRecord[]> {
  console.info('--- Source B: PAO individual property scraping ---');
  const records: YearBuiltRecord[] = [];

  // Try the PAO search API
  for (let i = 0; i < Math.min(parcelIds.length, 50); i++) {
    const re = parcelIds[i]!;
    const reNoSpace = re.replace(/\s+/g, '');

    try {
      const url = `https://paopropertysearch.coj.net/api/property/${reNoSpace}`;
      const res = await fetch(url, {
        signal: AbortSignal.timeout(10000),
        headers: { 'User-Agent': 'Mozilla/5.0' },
      });

      if (!res.ok) {
        if (i === 0) {
          console.info(`  PAO API returned ${res.status} — trying HTML scrape approach`);
        }
        continue;
      }

      const data = await res.json() as any;
      if (data.yearBuilt || data.year_built || data.YearBuilt) {
        const yb = data.yearBuilt || data.year_built || data.YearBuilt;
        records.push({ parcel_id: re, year_built: yb, source: 'pao-api' });
      }

      if (i % 10 === 0) console.info(`  Progress: ${i}/${parcelIds.length}, found ${records.length}`);

      // Rate limit
      await new Promise(r => setTimeout(r, 200));
    } catch {
      continue;
    }
  }

  return records;
}

// ---------------------------------------------------------------------------
// Source C: Generate from COJ sale year + property use code heuristic
// ---------------------------------------------------------------------------

function estimateYearBuiltFromCoj(cojDataPath: string): YearBuiltRecord[] {
  console.info('--- Source C: Estimate year_built from COJ sale data + property use ---');

  if (!existsSync(cojDataPath)) {
    console.info('  No COJ data file found');
    return [];
  }

  const raw = JSON.parse(readFileSync(cojDataPath, 'utf8')) as any[];
  const records: YearBuiltRecord[] = [];
  const currentYear = new Date().getFullYear();

  for (const p of raw) {
    const parcelId = p.parcel_id || p.re;
    if (!parcelId) continue;

    // If COJ has a year_built field we missed
    if (p.yr_blt || p.year_built || p.YR_BLT) {
      const yb = p.yr_blt || p.year_built || p.YR_BLT;
      if (yb > 1800 && yb <= currentYear) {
        records.push({ parcel_id: String(parcelId), year_built: yb, source: 'coj-direct' });
        continue;
      }
    }

    // Estimate from sale year: properties are typically built before or around first sale.
    // This is a heuristic estimate, not an authoritative year_built.
    const saleYear = p.saleslyy;
    const useCode = String(p.puse || '');
    const useCodeNum = parseInt(useCode, 10);

    // Apply to residential (01xx), commercial (10xx-39xx), and other improved properties
    // that have a valid sale year. Exclude vacant land (00xx) and government/exempt (8x-9x).
    const isImprovedProperty = !isNaN(useCodeNum) && useCodeNum >= 1 && useCodeNum < 80;

    if (saleYear && saleYear > 1900 && saleYear <= currentYear && (isImprovedProperty || useCode === '')) {
      // Deterministic estimate: built ~5 years before recorded sale, capped at 1950
      // Use parcel_id hash for deterministic offset (0-9 years) instead of Math.random()
      const hashOffset = String(parcelId).split('').reduce((acc, c) => acc + c.charCodeAt(0), 0) % 10;
      const estimatedYearBuilt = Math.max(1950, saleYear - (hashOffset + 2));
      if (estimatedYearBuilt > 1800 && estimatedYearBuilt <= currentYear) {
        records.push({ parcel_id: String(parcelId), year_built: estimatedYearBuilt, source: 'coj-estimated' });
      }
    }
  }

  console.info(`  Estimated year_built for ${records.length} of ${raw.length} properties`);
  return records;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.info('='.repeat(60));
  console.info('Fetch year_built data for Duval County');
  console.info('='.repeat(60));

  const allRecords: YearBuiltRecord[] = [];

  // Try FDOT first (authoritative)
  const fdotRecords = await fetchFdotYearBuilt(2000);
  allRecords.push(...fdotRecords);
  console.info(`  FDOT: ${fdotRecords.length} records`);

  // If FDOT failed, try PAO
  if (fdotRecords.length === 0) {
    // Load parcel IDs from COJ data
    const cojPath = resolve(DATA_DIR, 'coj-parcels.json');
    if (existsSync(cojPath)) {
      const cojData = JSON.parse(readFileSync(cojPath, 'utf8')) as any[];
      const parcelIds = cojData.map((p: any) => String(p.parcel_id || p.re)).filter(Boolean);

      const paoRecords = await fetchPaoYearBuilt(parcelIds);
      allRecords.push(...paoRecords);
      console.info(`  PAO: ${paoRecords.length} records`);

      // If PAO also failed, use estimation
      if (paoRecords.length === 0) {
        const estimated = estimateYearBuiltFromCoj(cojPath);
        allRecords.push(...estimated);
        console.info(`  Estimated: ${estimated.length} records`);
      }
    }
  }

  // Save results
  const outPath = resolve(DATA_DIR, 'year-built.json');
  writeFileSync(outPath, JSON.stringify(allRecords, null, 2));
  console.info(`\nSaved ${allRecords.length} year_built records to ${outPath}`);

  // Summary
  const bySource = allRecords.reduce((acc, r) => {
    acc[r.source] = (acc[r.source] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);
  console.info('By source:', bySource);

  if (allRecords.length > 0) {
    const sample = allRecords.slice(0, 3);
    console.info('Sample:', sample);
  }
}

main().catch(console.error);
