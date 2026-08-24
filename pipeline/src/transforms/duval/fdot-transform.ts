/**
 * COJ parcel data transform — normalize real City of Jacksonville ArcGIS data
 * into Property Record schema.
 *
 * Maps COJ parcel attributes (from coj-parcels.json) to lexicon-aligned fields.
 * Handles Web Mercator (EPSG:3857) → WGS84 (EPSG:4326) coordinate conversion.
 *
 * Data source: COJ ArcGIS REST Services
 */

import type {
  RawRecord,
  TransformResult,
  PropertyRecord,
  Address,
  Owner,
  Coordinates,
  Lot,
  Structure,
  Tax,
  DerivedSignals,
} from '../../lib/types.js';
import { computeProximitySignals } from './proximity-signals.js';

const CURRENT_YEAR = new Date().getFullYear();

// Duval County local cities for regional owner detection
const LOCAL_CITIES = new Set([
  'jacksonville', 'jacksonville beach', 'neptune beach', 'atlantic beach',
  'ponte vedra', 'ponte vedra beach', 'orange park', 'fleming island',
  'green cove springs', 'fernandina beach', 'yulee', 'callahan',
  'baldwin', 'middleburg',
]);

/**
 * Convert Web Mercator (EPSG:3857) coordinates to WGS84 (EPSG:4326) lat/lon.
 */
function webMercatorToLatLon(x: number, y: number): { lat: number; lng: number } {
  const lng = (x / 20037508.34) * 180;
  const lat = Math.atan(Math.exp((y / 20037508.34) * Math.PI)) * 360 / Math.PI - 90;
  return { lat, lng };
}

/**
 * Determine if an owner is non-local (regional/out-of-area investor).
 * Returns true if the mailing address is outside the Jacksonville metro area.
 */
function isRegionalOwner(mailCity: string | null | undefined, mailState: string | null | undefined): boolean {
  if (!mailCity && !mailState) return false;
  if (mailState && mailState.toLowerCase() !== 'fl') return true;
  if (mailCity && !LOCAL_CITIES.has(mailCity.toLowerCase().trim())) return true;
  return false;
}

/**
 * Infer water proximity from FEMA flood zone designation.
 * Properties in flood zones are typically near water bodies.
 */
function inferWaterProximity(floodZone: string | null | undefined): {
  waterProximityFt: number | undefined;
  isWaterfront: boolean;
} {
  if (!floodZone) return { waterProximityFt: undefined, isWaterfront: false };

  const fz = floodZone.toUpperCase();

  // VE/V zones: coastal high-hazard, typically waterfront
  if (fz.includes('VE') || fz.startsWith('V')) {
    return { waterProximityFt: 100, isWaterfront: true };
  }
  // AE/A zones: 100-year floodplain, near water but not necessarily waterfront
  if (fz.includes('AE') || fz.startsWith('A')) {
    return { waterProximityFt: 500, isWaterfront: false };
  }
  // X (shaded): 500-year floodplain
  if (fz.includes('SHADED') || fz.includes('0.2')) {
    return { waterProximityFt: 2000, isWaterfront: false };
  }
  // "NOT IN FLOOD ZONE" or X (unshaded)
  return { waterProximityFt: undefined, isWaterfront: false };
}

/**
 * Transform COJ (City of Jacksonville) parcel records into Property Record fields.
 * Accepts records with source_id 'coj-duval-parcels' or 'fdot-duval-parcels'.
 */
export function transformFdotRecords(records: RawRecord[]): TransformResult[] {
  return records
    .filter((r) => r.source_id === 'coj-duval-parcels' || r.source_id === 'fdot-duval-parcels')
    .map((record) => {
      const d = record.raw_data;

      // --- Address ---
      // COJ fields (lowercased from ArcGIS): longname (full street address), stnm_type (street type suffix e.g. "ST"),
      //   addrcity, zipcode, stno (street number), stname (street name)
      // FDOT fields: address_street, address_city, address_state, address_zip
      //
      // COJ "longname" is the full situs address (e.g. "3128 ATLANTIC BLVD").
      // COJ "stnm_type" is only the suffix (e.g. "ST", "AVE") — NOT a full street name.
      // FDOT "address_street" (APTS_STRT) is the full situs street address.
      const cojStreet = d.longname as string | null;
      const fdotStreet = d.address_street as string | null;
      // Build full street from COJ components if longname is missing
      const cojStNo = d.stno as string | number | null;
      const cojStName = d.stname as string | null;
      const cojStType = d.stnm_type as string | null;
      const cojComposed = (cojStNo && cojStName)
        ? [String(cojStNo), cojStName, cojStType].filter(Boolean).join(' ')
        : null;
      const streetAddr = cojStreet ?? cojComposed ?? fdotStreet;
      const city = (d.addrcity as string) || (d.address_city as string) || 'JACKSONVILLE';
      const zip = (d.zipcode ?? d.address_zip) != null ? String(d.zipcode ?? d.address_zip) : undefined;

      const address: Address = {
        full: streetAddr ?? undefined,
        street: streetAddr ?? undefined,
        city,
        state: 'FL',
        zip,
      };

      // --- Owner ---
      // COJ fields: lnameowner, mailaddr1, mailcity, mailstate, mailzip
      // FDOT fields: owner_name, owner_address, owner_city, owner_state, owner_zip
      const ownerName = (d.lnameowner ?? d.owner_name) as string | null;
      const mailAddr1 = (d.mailaddr1 ?? d.owner_address ?? d.owner_addr1) as string | null;
      const mailCity = (d.mailcity ?? d.owner_city) as string | null;
      const mailState = (d.mailstate ?? d.owner_state) as string | null;
      const mailZip = (d.mailzip ?? d.owner_zip) != null ? String(d.mailzip ?? d.owner_zip) : undefined;

      const currentOwner: Owner | null = ownerName
        ? {
            owner_name: ownerName,
            mailing_address: {
              street: mailAddr1 ?? undefined,
              city: mailCity ?? undefined,
              state: mailState ?? undefined,
              zip: mailZip,
            },
          }
        : null;

      // --- Valuations ---
      // COJ: cama_val; FDOT: assessed_value, just_value
      const assessedValue = typeof d.cama_val === 'number'
        ? d.cama_val
        : typeof d.assessed_value === 'number'
          ? d.assessed_value
          : typeof d.just_value === 'number'
            ? d.just_value
            : null;
      const marketValue = typeof d.just_value === 'number' ? d.just_value : assessedValue;

      // --- Year Built ---
      // COJ: not available; FDOT: year_built (ACT_YR_BLT), effective_year_built (EFF_YR_BLT)
      const rawYearBuilt = typeof d.year_built === 'number' ? d.year_built : null;
      const rawEffYearBuilt = typeof d.effective_year_built === 'number' ? d.effective_year_built : null;
      const yearBuilt = (rawYearBuilt && rawYearBuilt > 1800 && rawYearBuilt <= CURRENT_YEAR)
        ? rawYearBuilt
        : (rawEffYearBuilt && rawEffYearBuilt > 1800 && rawEffYearBuilt <= CURRENT_YEAR)
          ? rawEffYearBuilt
          : null;

      // --- Structure ---
      // COJ: puse, descpu; FDOT: dor_use_code
      const useCode = (d.puse ?? d.dor_use_code) as string | null;
      const useDescription = d.descpu as string | null;
      const totalLivingArea = typeof d.total_living_area === 'number' ? d.total_living_area : null;

      const structure: Structure = {
        year_built: yearBuilt ?? undefined,
        sqft: totalLivingArea ?? undefined,
        use_code: useCode ?? undefined,
        use_description: useDescription ?? undefined,
      };

      // --- Coordinates (Web Mercator → WGS84) ---
      // COJ: x_wgs/y_wgs (Web Mercator); FDOT: lat/lng (already WGS84) or centroid object
      let coordinates: Coordinates | null = null;
      const xWgs = d.x_wgs as number | null;
      const yWgs = d.y_wgs as number | null;
      if (typeof xWgs === 'number' && typeof yWgs === 'number' && xWgs !== 0 && yWgs !== 0) {
        coordinates = webMercatorToLatLon(xWgs, yWgs);
      } else if (typeof d.lat === 'number' && (typeof d.lng === 'number' || typeof d.long === 'number') && d.lat !== 0) {
        // COJ provides lat/long; FDOT provides lat/lng from polygon centroid
        const lngVal = (typeof d.lng === 'number' ? d.lng : d.long) as number;
        if (lngVal !== 0) coordinates = { lat: d.lat as number, lng: lngVal };
      } else if (d.centroid && typeof (d.centroid as Record<string, unknown>).lat === 'number') {
        const c = d.centroid as { lat: number; lng: number };
        coordinates = { lat: c.lat, lng: c.lng };
      }

      // --- Lot ---
      // COJ: acres; FDOT: acreage
      const acres = typeof d.acres === 'number' ? d.acres : typeof d.acreage === 'number' ? d.acreage : null;
      const zoning = d.zon_label as string | null;
      const floodZone = d.fld_zone as string | null;

      const lot: Lot = {};
      if (acres && acres > 0) {
        lot.area_acres = acres;
        lot.area_sqft = Math.round(acres * 43560);
      }
      if (zoning) lot.zoning = zoning;

      // --- Tax ---
      const tax: Tax = {
        assessed_value: assessedValue ?? undefined,
      };

      // --- Derived Signals ---
      const regional = isRegionalOwner(mailCity, mailState);
      const { waterProximityFt, isWaterfront } = inferWaterProximity(floodZone);

      // Compute proximity signals from coordinates (transit, starbucks, water)
      const proxSignals = coordinates
        ? computeProximitySignals(coordinates)
        : {};

      // Compute roof age from year_built when available
      const roofAgeYears = yearBuilt ? CURRENT_YEAR - yearBuilt : undefined;

      // Compute ownership tenure from sale date when available
      // COJ fields: saleslyy (sale year), saleslmm (sale month), salesldd (sale day)
      // FDOT fields: sale_date (if present from DOR data)
      let ownershipTenureYears: number | undefined;
      const saleYearCoj = typeof d.saleslyy === 'number' ? d.saleslyy : null;
      const saleDateFdot = d.sale_date as string | number | null | undefined;

      if (saleYearCoj && saleYearCoj > 1900 && saleYearCoj <= CURRENT_YEAR) {
        ownershipTenureYears = CURRENT_YEAR - saleYearCoj;
      } else if (saleDateFdot) {
        const saleYear = typeof saleDateFdot === 'number'
          ? saleDateFdot
          : parseInt(String(saleDateFdot).slice(0, 4), 10);
        if (!isNaN(saleYear) && saleYear > 1900 && saleYear <= CURRENT_YEAR) {
          ownershipTenureYears = CURRENT_YEAR - saleYear;
        }
      }

      const derivedSignals: DerivedSignals = {
        is_regional_owner: regional,
        ownership_tenure_years: ownershipTenureYears,
        roof_age_years: roofAgeYears,
        // Flood-zone water proximity takes precedence over haversine
        water_proximity_ft: waterProximityFt ?? proxSignals.water_proximity_ft,
        is_waterfront: isWaterfront || proxSignals.is_waterfront || false,
        // Transit and Starbucks from proximity computation
        transit_distance_mi: proxSignals.transit_distance_mi,
        starbucks_distance_mi: proxSignals.starbucks_distance_mi,
        within_walking_transit: proxSignals.within_walking_transit,
        within_walking_starbucks: proxSignals.within_walking_starbucks,
      };

      const fields: Partial<PropertyRecord> = {
        address,
        assessed_value: assessedValue,
        market_value: marketValue,
        current_owner: currentOwner,
        structure,
        coordinates,
        lot,
        tax,
        derived_signals: derivedSignals,
      };

      return {
        parcel_id: record.parcel_id,
        fields,
      };
    });
}

export default transformFdotRecords;
