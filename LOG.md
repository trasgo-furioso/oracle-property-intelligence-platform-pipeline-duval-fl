# Oracle Pipeline — Development Log

## Session 1 — July 15

The upstream team created the repository with a bare README. No code, no structure — just a placeholder.

## Session 2 — August 19

The stakeholder spec landed, defining what the pipeline should do: ingest Duval County property records from public sources, enrich them with derived signals, publish to IPFS, and expose them through a search API and AI agent.

## Session 3 — August 21 (morning)

Built the entire platform in one sitting. Infrastructure went up on AWS (EC2, CloudFront, Amplify), the pipeline engine got six data source adapters, a workflow orchestrator, IPFS publishing via Filebase, an MCP server for machine-to-machine access, an AI-powered chat agent, and a four-page React dashboard. Filebase's free tier only allows one bucket and one IPNS name, which shaped the publishing design from the start.

## Session 4 — August 21 (afternoon)

Got the deployed system working end-to-end. CloudFront now routes API traffic to the EC2 backend, the ingestion trigger actually runs and publishes data, and the cheapest available AI model powers the agent chat. Docker builds, environment wiring, and deployment plumbing all came together.

## Session 5 — August 21 (evening)

Added Playwright end-to-end tests covering all four pages and all six property query types. These became the regression safety net for every session that followed.

## Session 6 — August 22 (early)

Removed the Postgres dependency for reads. Property search and the AI agent now query published Parquet files on IPFS directly through DuckDB — no database in the read path. Added production observability: structured logging, CloudWatch dashboards, PagerDuty alerting, and a CI pipeline.

## Session 7 — August 22 (mid-day)

Pivoted from mock data to real county records. The City of Jacksonville ArcGIS portal turned out to be geo-blocked from outside the US, so all data fetching had to run on the EC2 instance. The real field names bore no resemblance to the mock schema, requiring a full rewrite of the data transforms.

## Session 8 — August 22 (afternoon)

Scaled real data ingestion with paginated ArcGIS fetches and supplemental FDOT records merged by parcel ID. Fixed serialization issues in the agent's query layer and made all six search modes report honestly when properties lack the fields a query needs.

## Session 9 — August 23

Solved the missing year-built problem using a three-tier estimation fallback. Formalized the CRM integration contract: downstream systems discover fresh data through a single IPNS lookup that resolves to the latest Parquet file. Renamed columns to be self-documenting and added provenance tracking. Discovered that webhook notifications to the CRM existed only in dead code — wired them into the actual ingestion flow.

## Session 10 — August 24

Post-evaluation polish. Fixed address mapping issues, improved mock mode with real Jacksonville street names, and expanded year-built estimation coverage. Then raised the ingestion target from 2,000 to 400,000 properties for full county coverage. Hit memory limits immediately — first JSON parsing choked on 500MB payloads, then V8's string ceiling killed the streaming parser. Rewired ingestion to fetch live from ArcGIS with pagination, eliminating the need to hold all records in memory at once.

## Session 11 — August 25

Bumped the processing container's memory ceiling for full-county runs. Gave the dashboard a visual refresh — replaced the near-black theme with blue accents and semantic color coding for status indicators. Recorded the final demo with all 399,000 properties loaded. Added viewport filtering and multi-criteria search endpoints for richer CRM integration. Documented the architecture for handoff.
