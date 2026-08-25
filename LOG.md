# Oracle Pipeline — Development Log

## Session 1 — 2026-07-15

Upstream team created the repo with a bare README. No code yet.

## Session 2 — 2026-08-19

Stakeholder spec landed as a README update, defining what the pipeline should do.

## Session 3 — 2026-08-21

Bootstrapped the entire pipeline in one sitting — data adapters, workflow engine, dashboard, property search, agent chat, IPFS publishing. Filebase's free tier forced creative constraints: one bucket, one IPNS name, path prefixes for everything.

## Session 4 — 2026-08-21

Got the deployed system talking end-to-end. CloudFront fronts the EC2 backend, the AI agent runs on the cheapest Haiku model, and ingestion actually publishes to IPFS.

## Session 5 — 2026-08-21

Added Playwright e2e tests covering all four pages and six query types. This became the regression harness for every session that followed.

## Session 6 — 2026-08-22

Ripped out Postgres entirely and replaced it with DuckDB reading Parquet over IPFS. Property search and the AI agent now query published open data directly — no database in the loop. Added Powertools, PagerDuty, and CloudWatch.

## Session 7 — 2026-08-22

Pivoted from mock data to real county records. The ArcGIS portal is geo-blocked from Brazil, forcing all data operations onto the US-based EC2. COJ field names bore no resemblance to our assumptions, requiring a full transform rewrite.

## Session 8 — 2026-08-22

Scaled the real data ingestion — paginated ArcGIS fetches, merged FDOT supplement data by parcel ID. Fixed BigInt serialization in the agent's DuckDB queries.

## Session 9 — 2026-08-22

Made all six search modes honest about data gaps, reporting when properties lack the fields a query needs.

## Session 10 — 2026-08-23

Solved the missing year-built problem with a three-tier fallback: FDOT, property appraiser, then estimation from sale year. Formalized the CRM contract so downstream systems discover data through a single IPNS lookup.

## Session 11 — 2026-08-23

Renamed Parquet columns to be self-documenting, added provenance tracking, and wrapped deploys in a Makefile.

## Session 12 — 2026-08-23

Discovered webhook notifications only existed in a dead code path. Wired them into the actual ingestion flow so the CRM finally gets notified after pipeline runs.

## Session 13 — 2026-08-24

Post-evaluation polish. Fixed address mapping, improved mock mode with real Jacksonville streets, expanded year-built estimation, and raised the ingestion ceiling to 400k for full county coverage.

## Session 14 — 2026-08-24

Hit the 400k wall. JSON.parse choked on 500MB, streaming fixed that, then V8's string limit killed ingestion. Rewired the pipeline to fetch live from ArcGIS with pagination instead of reading a pre-fetched file, eliminating the memory bottleneck entirely.

## Session 15 — 2026-08-25

Bumped the Docker heap to 6GB for full-county processing. Gave the frontend a visual refresh — swapped the near-black theme for blue with proper status badges and hover states.
