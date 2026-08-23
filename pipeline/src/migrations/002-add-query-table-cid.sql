-- 002-add-query-table-cid.sql
-- Add query_table_cid column to pipeline_runs for CRM integration.
-- Stores the IPFS CID of the Parquet query table so it can be embedded
-- in index.json and exposed via the API.

ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS query_table_cid TEXT;
