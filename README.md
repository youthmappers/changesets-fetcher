# YouthMappers Changesets Fetcher

An automated weekly data pipeline that pulls YouthMappers changesets from the OSM Public Dataset (Athena), enriches and aggregates them (DuckDB), and publishes GeoParquet, CSV, JSON, and PMTiles used by the YouthMappers Activity Dashboard.

[![Run Athena Queries](https://github.com/youthmappers/changesets-fetcher/actions/workflows/run-athena.yml/badge.svg)](https://github.com/youthmappers/changesets-fetcher/actions/workflows/run-athena.yml)

[![Build files for Activity Dashboard](https://github.com/youthmappers/changesets-fetcher/actions/workflows/run-duckdb.yml/badge.svg?branch=main)](https://github.com/youthmappers/changesets-fetcher/actions/workflows/run-duckdb.yml)

## How the repository works (end-to-end)

1. **Athena (weekly)** builds external tables and runs a large query against OSM Public Dataset tables and the YouthMappers members dataset, writing partitioned Parquet to S3.
2. **DuckDB (weekly)** reads the newest partition from S3, performs spatial and H3 aggregations, and produces all downstream files for the dashboard.
3. **Tiling + publish** converts GeoJSONSeq into PMTiles and pushes everything to S3, then invalidates CloudFront.

All of the above is automated via GitHub Actions.

## Automation (GitHub Actions)

- **Run Athena Queries**: [ .github/workflows/run-athena.yml ](.github/workflows/run-athena.yml)
	- Schedule: Sundays at 10:00 UTC.
	- Runs the Athena pipeline entry point: `changesets_fetcher.athena:main` (installed as `athena`).
	- Uses OIDC to assume the AWS role in `YOUTHMAPPERS_AWS_ROLE`.

- **Build files for Activity Dashboard**: [ .github/workflows/run-duckdb.yml ](.github/workflows/run-duckdb.yml)
	- Schedule: Sundays at 10:30 UTC (after Athena completes).
	- Runs the DuckDB pipeline entry point: `changesets_fetcher.duckdb:main` (installed as `duckdb`).
	- Runs tiling and publish scripts in sequence.

## Athena pipeline

**Implementation**: [src/changesets_fetcher/athena.py](src/changesets_fetcher/athena.py)

### Steps

1. **Create database** if missing.
2. **Create external tables** defined in [sql/tables.sql](sql/tables.sql):
	 - `changesets` from s3://osm-pds/changesets
	 - `planet_history` from s3://osm-pds/planet-history
	 - `planet` from s3://osm-pds/planet
	 - `natural_earth_admin0` from s3://youthmappers-internal-us-east1/country_boundaries/
	 - `youthmappers` from s3://youthmappers-internal-us-east1/mappers (partitioned by `ds`)
3. **Add Glue partitions** for the YouthMappers members dataset.
4. **Run the main YouthMappers query** in [sql/youthmappers_query.sql](sql/youthmappers_query.sql) and `UNLOAD` to:
	 - s3://youthmappers-internal-us-east1/youthmappers_changesets/
	 - Partitioned by `ds` and compressed as ZSTD Parquet.

### Configuration

Athena settings are read from environment variables (with defaults):

- `YOUTHMAPPERS_AWS_REGION` (default us-east-1)
- `YOUTHMAPPERS_ATHENA_DATABASE` (default youthmappers)
- `YOUTHMAPPERS_ATHENA_WORKGROUP` (default youthmappers)
- `YOUTHMAPPERS_ATHENA_OUTPUT_LOCATION` (default s3://youthmappers-internal-us-east1/athena-results/)
- `YOUTHMAPPERS_AWS_ROLE` (OIDC-assumed role in GitHub Actions)

## DuckDB pipeline

**Implementation**: [src/changesets_fetcher/duckdb.py](src/changesets_fetcher/duckdb.py)

### Steps

1. **Initialize DuckDB** with `spatial` and `h3` extensions and configure S3 credential chain.
2. **Detect the latest partition (`ds`)** from the Athena output in:
	 - s3://youthmappers-internal-us-east1/youthmappers_changesets/
3. **Build working tables** inside DuckDB:
	 - `changesets` (latest ds, plus H3 index)
	 - `youthmappers` (members dataset for the matching ds)
	 - `natural_earth` (local file: ne_adm0.parquet)
	 - `ym_changesets` (joined/enriched table)
4. **Aggregate and export** using query blocks in [sql/duckdb.sql](sql/duckdb.sql).

### Outputs (written to output/)

- `latest_ds.txt` (latest partition used)
- `weekly_chapter_activity.csv` (weekly rollup by chapter)
- `activity.json` (chapter list + ds for the dashboard)
- `monthly_activity_all_time.json`
- `top_edited_countries.json`
- `h3_4_weekly.geojsonseq`
- `h3_6_weekly.geojsonseq`
- `daily_editing_per_user.geojsonseq`
- `daily_bboxes.geojsonseq`
- `daily_rollup.parquet` (aggregated anonymized rollup)

## Tiling + publish

- **Tiling**: [scripts/tile.sh](scripts/tile.sh)
	- Uses `tippecanoe` to build PMTiles from the GeoJSONSeq outputs.
	- Produces: res4.pmtiles, res6.pmtiles, res8.pmtiles, res8_bboxes.pmtiles.

- **Publish to S3**: [scripts/copy-to-s3.sh](scripts/copy-to-s3.sh)
	- Copies outputs to s3://youthmappers-usw2/activity-dashboard/ds=LATEST_DS/...
	- Writes activity.json to s3://youthmappers-usw2/activity-dashboard/activity.json
	- Writes daily_rollup.parquet to s3://youthmappers-usw2/activity/daily_rollup.parquet
	- Invalidates CloudFront distribution E6U9U7HMQT3MF for /activity-dashboard/activity.json

## SQL inventory

- [sql/tables.sql](sql/tables.sql) creates external Athena tables.
- [sql/youthmappers_query.sql](sql/youthmappers_query.sql) is the main Athena query that joins changesets with planet history statistics and country boundaries, then unloads to Parquet.
- [sql/duckdb.sql](sql/duckdb.sql) contains named query blocks used for aggregations and exports.

## Project entry points

Defined in [pyproject.toml](pyproject.toml):

- `athena` → `changesets_fetcher.athena:main`
- `duckdb` → `changesets_fetcher.duckdb:main`

## Inputs and dependencies

- **AWS S3 sources**: OSM Public Dataset, YouthMappers members dataset, Natural Earth boundaries.
- **Python**: 3.11 (managed with uv in CI).
- **DuckDB extensions**: `spatial` and `h3`.
- **Tippecanoe**: required for PMTiles generation.

## Repository layout

- [.github/workflows](.github/workflows) — automation schedules and execution steps.
- [src/changesets_fetcher](src/changesets_fetcher) — Athena + DuckDB pipeline code.
- [sql](sql) — Athena and DuckDB SQL blocks.
- [scripts](scripts) — tiling and publish scripts.
- [output](output) — generated artifacts (checked in only for reference).
