/**
 * Duval Property Appraiser portal source adapter.
 * T019 — Playwright-based browser flow to extract property details.
 */

import type { SourceAdapter, RawRecord } from '../lib/types.js';

const SOURCE_ID = 'duval-appraiser';
const BASE_URL = 'https://paopropertysearch.coj.net';

// Rate limiting
const REQUEST_DELAY_MS = 1500;
const MAX_RETRIES = 3;

/**
 * Sleep for a given number of milliseconds.
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Fetch property details from the Duval Property Appraiser portal
 * for a single parcel ID using Playwright browser automation.
 */
async function fetchParcel(parcelId: string, retryCount = 0): Promise<RawRecord | null> {
  try {
    // Dynamic import of Playwright to avoid loading at module level
    const { chromium } = await import('playwright');

    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    try {
      // Navigate to search page
      await page.goto(`${BASE_URL}/Basic/Search`, { waitUntil: 'domcontentloaded' });

      // Search by RE number / parcel ID
      await page.fill('input[name="RealEstateNumber"], input#txtSearchBox, input[type="text"]', parcelId);
      await page.click('button[type="submit"], input[type="submit"], .search-btn');

      // Wait for results
      await page.waitForTimeout(2000);

      // Extract property data from the results page
      const rawData = await page.evaluate(() => {
        const getText = (selector: string) => {
          const el = document.querySelector(selector);
          return el?.textContent?.trim() ?? '';
        };

        const getAllText = (selector: string) => {
          return Array.from(document.querySelectorAll(selector)).map(
            (el) => (el as HTMLElement).textContent?.trim() ?? '',
          );
        };

        // Try common data selectors from the appraiser portal
        const tables = document.querySelectorAll('table');
        const data: Record<string, string> = {};

        tables.forEach((table: Element) => {
          const rows = table.querySelectorAll('tr');
          rows.forEach((row: Element) => {
            const cells = row.querySelectorAll('td, th');
            if (cells.length >= 2) {
              const key = cells[0]?.textContent?.trim() ?? '';
              const value = cells[1]?.textContent?.trim() ?? '';
              if (key && value) {
                data[key] = value;
              }
            }
          });
        });

        return {
          pageTitle: document.title,
          bodyText: document.body.innerText.substring(0, 5000),
          extractedData: data,
          owner: getText('.owner-name, .OwnerName, [data-field="owner"]'),
          address: getText('.property-address, .SiteAddress, [data-field="address"]'),
          assessedValue: getText('.assessed-value, [data-field="assessed"]'),
          yearBuilt: getText('.year-built, [data-field="yearBuilt"]'),
          sqft: getText('.sqft, [data-field="sqft"]'),
          saleHistory: getAllText('.sale-row, .SaleHistory tr'),
        };
      });

      return {
        parcel_id: parcelId,
        source_id: SOURCE_ID,
        raw_data: rawData,
      };
    } finally {
      await browser.close();
    }
  } catch (err) {
    if (retryCount < MAX_RETRIES) {
      console.warn(
        `[${SOURCE_ID}] Retry ${retryCount + 1}/${MAX_RETRIES} for parcel ${parcelId}: ${err}`,
      );
      await sleep(REQUEST_DELAY_MS * (retryCount + 1));
      return fetchParcel(parcelId, retryCount + 1);
    }
    console.error(`[${SOURCE_ID}] Failed to fetch parcel ${parcelId} after ${MAX_RETRIES} retries:`, err);
    return null;
  }
}

/**
 * Appraiser source adapter implementation.
 */
export const appraiserAdapter: SourceAdapter = {
  source_id: SOURCE_ID,

  async fetch(parcelIds?: string[], options?: { limit?: number }): Promise<RawRecord[]> {
    if (!parcelIds || parcelIds.length === 0) {
      console.warn(`[${SOURCE_ID}] No parcel IDs provided for appraiser fetch`);
      return [];
    }

    const limit = options?.limit ?? parcelIds.length;
    const idsToFetch = parcelIds.slice(0, limit);
    const results: RawRecord[] = [];

    console.info(`[${SOURCE_ID}] Fetching ${idsToFetch.length} parcels from appraiser portal`);

    for (const parcelId of idsToFetch) {
      const record = await fetchParcel(parcelId);
      if (record) {
        results.push(record);
      }
      // Rate limiting
      await sleep(REQUEST_DELAY_MS);
    }

    console.info(`[${SOURCE_ID}] Fetched ${results.length}/${idsToFetch.length} parcels`);
    return results;
  },
};

/**
 * Generate mock appraiser data for testing without browser automation.
 */
export function generateMockAppraiserRecord(parcelId: string): RawRecord {
  const yearBuilt = 1985 + Math.floor(Math.random() * 35);
  const sqft = 1000 + Math.floor(Math.random() * 3000);
  const assessedValue = 100000 + Math.floor(Math.random() * 500000);

  // Use realistic Jacksonville street names instead of generic "Main St"
  const STREETS = [
    'Atlantic Blvd', 'Beach Blvd', 'University Blvd', 'San Jose Blvd',
    'Baymeadows Rd', 'Hendricks Ave', 'Riverside Ave', 'Park St',
    'Philips Hwy', 'Blanding Blvd', 'Normandy Blvd', 'Edgewood Ave',
    'King St', 'Duval St', 'Ocean St', 'Herschel St',
    'Post St', 'Margaret St', 'Forbes St', 'Stockton St',
  ];
  const street = STREETS[Math.floor(Math.random() * STREETS.length)];

  return {
    parcel_id: parcelId,
    source_id: SOURCE_ID,
    raw_data: {
      owner: `Owner-${parcelId}`,
      address: `${Math.floor(Math.random() * 9999)} ${street}, Jacksonville, FL`,
      assessed_value: assessedValue,
      market_value: Math.round(assessedValue * 1.15),
      year_built: yearBuilt,
      sqft,
      stories: Math.random() > 0.5 ? 2 : 1,
      bedrooms: 2 + Math.floor(Math.random() * 4),
      bathrooms: 1 + Math.floor(Math.random() * 3),
      roof_type: ['shingle', 'tile', 'metal', 'flat'][Math.floor(Math.random() * 4)],
      construction_type: ['frame', 'masonry', 'concrete block'][Math.floor(Math.random() * 3)],
      use_code: 'R1',
      use_description: 'Single Family Residential',
      last_sale_date: `${2010 + Math.floor(Math.random() * 14)}-${String(1 + Math.floor(Math.random() * 12)).padStart(2, '0')}-${String(1 + Math.floor(Math.random() * 28)).padStart(2, '0')}`,
      last_sale_price: Math.round(assessedValue * (0.8 + Math.random() * 0.4)),
      tax_year: 2025,
      taxable_value: Math.round(assessedValue * 0.9),
      annual_tax: Math.round(assessedValue * 0.012),
    },
  };
}
