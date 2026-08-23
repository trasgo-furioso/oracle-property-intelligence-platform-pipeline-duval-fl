-- 001-initial-schema.sql
-- T012 — Pipeline database schema: pipeline_runs, data_sources, properties, run_sources

-- Enum types
DO $$ BEGIN
  CREATE TYPE pipeline_run_status AS ENUM ('running', 'success', 'partial', 'failed');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE source_category AS ENUM ('property', 'permit', 'ownership', 'business', 'contractor', 'location');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE collection_method AS ENUM ('browser-flow', 'api', 'bulk-download', 'scrape');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE run_source_status AS ENUM ('pending', 'running', 'success', 'partial', 'failed', 'skipped');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ---------------------------------------------------------------------------
-- pipeline_runs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  county              TEXT NOT NULL DEFAULT 'duval',
  started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at        TIMESTAMPTZ,
  status              pipeline_run_status NOT NULL DEFAULT 'running',
  record_count        INTEGER NOT NULL DEFAULT 0,
  delta_new           INTEGER NOT NULL DEFAULT 0,
  delta_updated       INTEGER NOT NULL DEFAULT 0,
  delta_removed       INTEGER NOT NULL DEFAULT 0,
  source_limitations  JSONB NOT NULL DEFAULT '[]'::jsonb,
  published_artifact_cid TEXT,
  ipns_pointer        TEXT,
  query_table_cid     TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_county_started
  ON pipeline_runs (county, started_at DESC);

-- ---------------------------------------------------------------------------
-- data_sources
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_sources (
  source_id           TEXT PRIMARY KEY,
  name                TEXT NOT NULL,
  category            source_category NOT NULL,
  url                 TEXT NOT NULL,
  collection_method   collection_method NOT NULL,
  last_successful_run TIMESTAMPTZ,
  record_count        INTEGER NOT NULL DEFAULT 0,
  limitations         TEXT
);

-- ---------------------------------------------------------------------------
-- properties
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS properties (
  uuid                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  parcel_id           TEXT UNIQUE NOT NULL,
  address             JSONB NOT NULL DEFAULT '{}'::jsonb,
  county_jurisdiction TEXT NOT NULL DEFAULT 'duval',
  assessed_value      NUMERIC,
  market_value        NUMERIC,
  ownership           JSONB NOT NULL DEFAULT '[]'::jsonb,
  current_owner       JSONB,
  permits             JSONB NOT NULL DEFAULT '[]'::jsonb,
  structure           JSONB NOT NULL DEFAULT '{}'::jsonb,
  lot                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  coordinates         JSONB,
  tax                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  provenance          JSONB NOT NULL DEFAULT '{}'::jsonb,
  derived_signals     JSONB NOT NULL DEFAULT '{}'::jsonb,
  content_hash        TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_properties_parcel_id ON properties (parcel_id);
CREATE INDEX IF NOT EXISTS idx_properties_county ON properties (county_jurisdiction);
CREATE INDEX IF NOT EXISTS idx_properties_assessed_value ON properties (assessed_value);

-- GIN indexes for JSONB queries on derived signals
CREATE INDEX IF NOT EXISTS idx_properties_derived_signals
  ON properties USING GIN (derived_signals);

-- ---------------------------------------------------------------------------
-- run_sources  (join table: pipeline_runs <-> data_sources)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS run_sources (
  run_id              UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
  source_id           TEXT NOT NULL REFERENCES data_sources(source_id) ON DELETE CASCADE,
  records_ingested    INTEGER NOT NULL DEFAULT 0,
  duration_ms         INTEGER,
  status              run_source_status NOT NULL DEFAULT 'pending',
  limitations         TEXT,
  PRIMARY KEY (run_id, source_id)
);
