# Oracle Pipeline — Development Log

## Session 1 — 2026-07-15

Repository created by the upstream team with a bare README. No code yet — just a placeholder.

## Session 2 — 2026-08-19

The stakeholder spec landed as a README update, kicking off a three-day gap before coding began.

## Session 3 — 2026-08-21 (~3.5h)

Built the entire pipeline skeleton in one marathon session — six data source adapters, a workflow engine, a dashboard UI, property search, agent chat, and IPFS publishing. The Filebase free tier only allows one bucket and one IPNS name, so we had to use path prefixes instead of separate buckets per artifact type. Docker builds broke because cross-package TypeScript configs couldn't resolve inside the container, so we inlined every tsconfig.

## Session 4 — 2026-08-21 (~2h)

Got the deployed system talking end-to-end. The frontend was wired through CloudFront so it could reach the EC2 backend without hardcoded URLs. A subtle but deployment-breaking discovery: Filebase's endpoint is `s3.filebase.io`, not `s3.filebase.com`, and its region must be `auto`. The session ended with the AI agent pinned to the cheapest Haiku model and ingestion actually publishing to IPFS.

## Session 5 — 2026-08-21 (~0h)

Added Playwright end-to-end tests covering all four pages — dashboard, pipeline runs, property search (all six query types), and agent chat. This became the regression harness for every session that followed.

## Session 6 — 2026-08-22 (~3.5h)

Pivoted the entire data layer from Postgres to DuckDB reading Parquet files over IPFS. This meant both property search and the AI agent now query published open data directly — no database in the loop. IPNS sub-paths returned 404, so we switched to direct CID resolution. Also wired in Powertools observability, PagerDuty alerting, CloudWatch dashboards, and 13 API acceptance tests.

## Session 7 — 2026-08-22 (~2h)

Started the real data pivot. Built a fetcher for COJ ArcGIS and FDOT, but hit immediate roadblocks: the FDOT URL needed a specific layer number for Duval County, and COJ's field names were completely different from what we'd assumed, requiring a full transform rewrite. We also discovered that Jacksonville's data portals are geo-blocked from Brazil, so all fetching has to run on the US-based EC2 instance. Filebase's 500-pin free-tier limit forced us to drop per-property JSON uploads in favor of bulk Parquet only.

## Session 8 — 2026-08-22 (~2h)

Continued scaling real data. Upgraded the ArcGIS fetcher to paginate in 1,000-record batches and merged FDOT supplement data (year built, square footage, sale dates) into the COJ records by parcel ID. Fixed the agent chat's DuckDB queries to handle BigInt serialization and updated the e2e tests to accept real parcel ID formats.

## Session 9 — 2026-08-22 (~0h)

Quick fix to add data gap transparency to the remaining two query types, so all six search modes now report when properties lack the data needed for that query.

## Session 10 — 2026-08-23 (~1h)

Tackled the missing year-built data with a three-tier strategy: try FDOT first, then the property appraiser, then estimate from sale year and property use code. Also formalized the CRM integration contract — the published index.json now embeds the Parquet CID so the downstream CRM can discover data through a single IPNS lookup.

## Session 11 — 2026-08-23 (~0h)

Cleaned up the Parquet schema for clarity — renamed columns to be self-documenting and added provenance tracking fields. Added a Makefile so deploys and verifications became one-liners instead of multi-step rituals.

## Session 12 — 2026-08-23 (~0h)

Wired webhook notifications into the actual ingestion flow. They had only existed in an unused Restate workflow path — the CRM was never getting notified after pipeline runs until this fix.

## Session 13 — 2026-08-24 (~1.5h)

Post-evaluation polish. Recorded a Playwright demo video, fixed address mapping to use the correct COJ field, improved mock mode with real Jacksonville street names, and expanded year-built estimation to cover all improved property types. Raised the ingestion limit from 200 to 400,000 for full Duval County coverage.

## Session 14 — 2026-08-24 (~3h)

Scaled from 2,000 to 400,000 parcels and hit a wall. JSON.parse crashed on the 500MB file, so we switched to streaming. Then the ingestion itself overflowed V8's string limit, so we converted that to streaming too. Finally rewired the pipeline to fetch live from the ArcGIS API with pagination at ingestion time instead of reading a pre-fetched file, eliminating the memory bottleneck entirely.

## Session 15 — 2026-08-25 (~1h)

Final polish. Bumped the Node.js heap to 6GB in Docker for 400k-record processing. Gave the frontend a visual refresh — swapped the near-black theme for blue, added semantic status badges, and put proper hover states on all buttons.
