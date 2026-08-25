# Implementation Guide

## Architecture

The pipeline runs as a Docker Compose stack on EC2 with three services:

- **Hono API** (`:9080`): REST endpoints for pipeline triggers, status, and agent chat
- **Restate SDK** (`:9081`): Durable workflow orchestration for multi-step ingestion
- **Postgres**: Pipeline run metadata and state tracking

**CloudFront** fronts the EC2 instance to provide HTTPS without a custom domain. Routes `/api/*` to the EC2 origin.

### Data Sources

Six data sources feed the pipeline:

1. **COJ Parcels** — Live ArcGIS pagination from `maps.coj.net` (primary, 399,999 parcels)
2. **COJ Permits** — Building permit history
3. **BBB** — Better Business Bureau contractor ratings
4. **Sunbiz** — Florida business entity records
5. **Business** — Local business registrations
6. **Contractors** — Licensed contractor database

### IPFS/IPNS Publishing

- **Filebase** S3-compatible API at `s3.filebase.io` (region `auto`)
- Single IPFS bucket `elephant-oracle-duval` with path prefixes (`open-data/`, `query-tables/`)
- **IPNS stable pointer** resolves to `index.json`, which contains `query_table_cid` pointing to the current Parquet file
- CRM discovers data via: `IPNS → index.json → query_table_cid → Parquet`

### Agent Queries

- DuckDB with `httpfs` extension reads Parquet directly from IPFS gateway URLs
- Agent chat powered by Claude Haiku, translates natural language to DuckDB SQL
- CID resolution uses direct CID (not IPNS sub-path) to avoid gateway caching issues

### Derived Data

- **Year-built estimation**: 1,460 properties estimated from COJ sale year + property use code heuristic (FDOT source requires auth token)
- **Proximity signals**: Computed from hardcoded Jacksonville reference data (20 transit stops, 15 Starbucks, 21 water features) plus COJ flood zone field

## Key URLs

| Resource | URL |
|----------|-----|
| Frontend | https://d5sfa8vgu8mcx.cloudfront.net |
| API | https://d5sfa8vgu8mcx.cloudfront.net/api |
| API Gateway (Lambda) | https://k9f346jdz9.execute-api.us-east-2.amazonaws.com/v1/ |
| IPNS | `k51qzi5uqu5dggq0h9xylfc0kr0kpw7i4zcacnfrymz9sjv7mpeze4femaujcz` |

## Data Flow

```
COJ ArcGIS → Live Fetch (pagination) → Transform → Reconcile → DuckDB → Parquet → Filebase IPFS → IPNS publish → Webhook to CRM
```

Each pipeline run:
1. Paginates COJ ArcGIS (1,000 records per page) to fetch all Duval County parcels
2. Transforms raw GIS features into normalized property records
3. Reconciles with existing data (dedup, merge updates)
4. Loads into DuckDB for query-table generation
5. Exports Parquet and publishes to Filebase IPFS
6. Updates IPNS pointer to new `index.json`
7. Sends webhook to CRM with new CID

## Stats

- **399,999** properties published
- **6 query types**: `roof_age`, `water_view`, `ownership_tenure`, `regional_owners`, `transit_walking`, `starbucks_walking`
- **285** unit tests (Vitest)
- **13** e2e tests (Playwright, all passing with video capture)

## Infrastructure

- **Region**: us-east-2
- **EC2**: `i-00bb59df78fecdfdd` (app dir: `/opt/app`)
- **CloudFront**: Distribution fronting EC2
- **Lambda**: `oracle-pipeline-duval-agent`, `oracle-pipeline-duval-mcp`
- **Agent model**: `claude-haiku-4-5-20251001`

## Kit Agents Used

| Agent | Role |
|-------|------|
| `oracle` | 14-step ingestion pipeline orchestration |
| `metagross` | Monorepo scaffolding and structure |
| `donphan` | MCP integration for Oracle open-data |
| `ash` | AI agent chat implementation |
| `apply-engineering-guidelines` | Baseline skill (Powertools, PagerDuty, CloudWatch, Vitest) |

## Root Repo

This repo is a git submodule of the [Elephant orchestration repo](https://github.com/trasgo-furioso/elephant), which manages specs, e2e tests, and submodule pointers for the integrated Oracle + CRM system.
