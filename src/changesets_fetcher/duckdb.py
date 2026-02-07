"""DuckDB query processor for local data analysis."""

import argparse
import logging
import os
import sys
from pathlib import Path
import json
import numpy as np
import pandas as pd
from datetime import datetime

import duckdb

from .utils import load_query_from_path

logger = logging.getLogger(__name__)

class DuckDBQueries:
    """Execute DuckDB queries for local data processing."""

    def __init__(
        self,
        database_path: str = ":memory:",
        output_dir: str = "output",
    ):
        """Initialize the DuckDB Query wrapper.

        Args:
            database_path: Path to DuckDB database file (":memory:" for in-memory)
            output_dir: Directory for output files
        """
        self.database_path = database_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(database_path)
        logger.info(f"Connected to DuckDB at {database_path}")
        logger.info(f"Output directory: {self.output_dir}")

        # Load spatial & h3 extensions
        self.conn.execute("""
            INSTALL spatial;
            LOAD spatial;
            INSTALL h3 FROM community;
            LOAD h3;""")

        # Set S3 credentials and such
        self.conn.execute("""CREATE OR REPLACE SECRET my_s3_secret (
            TYPE s3,
            PROVIDER credential_chain,
            REGION 'us-east-1'
        );""")
        self.conn.execute("SET s3_region='us-east-1';")

        def _mask(value: str | None) -> str:
            if not value:
                return "<unset>"
            if len(value) <= 8:
                return "***"
            return f"{value[:4]}...{value[-4:]}"

        logger.info(
            "AWS env: AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_SESSION_TOKEN=%s",
            _mask(os.getenv("AWS_ACCESS_KEY_ID")),
            _mask(os.getenv("AWS_SECRET_ACCESS_KEY")),
            _mask(os.getenv("AWS_SESSION_TOKEN")),
        )

    def run_query(
        self,
        query: str | None = None,
        query_path: str | None = None,
        query_name: str = "query",
        prefix: str | None = None,
        suffix: str | None = None,
    ) -> None:
        """Run a single DuckDB query.

        Args:
            query: SQL query string.
            query_path: "filename.sql" or "filename.sql:query-name".
            query_name: Name for logging purposes.
            prefix: Optional SQL to prepend (query will be wrapped in parentheses).
            suffix: Optional SQL to append (query will be wrapped in parentheses).
        """
        if isinstance(query, str):
            resolved_query = query
        elif query_path:
            resolved_query = load_query_from_path(query_path)
        else:
            raise ValueError("Provide either query (string) or query_path (filename:query-name)")

        if prefix or suffix:
            prefix_sql = (prefix or "").strip()
            suffix_sql = (suffix or "").strip()
            resolved_query = f"{prefix_sql} ({resolved_query}) {suffix_sql}".strip()

        logger.info(f"{query_name}")
        stmt_preview = resolved_query[:100].replace("\n", " ")
        logger.debug(f"Query preview: {stmt_preview}...")

        result = self.conn.execute(resolved_query)

        return result

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s::%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> None:
    """Main entry point for DuckDB processing."""
    parser = argparse.ArgumentParser(
        description="YouthMappers DuckDB pipeline - explicit query execution"
    )
    parser.add_argument(
        "--database-path",
        default="ym.ddb",
        help="DuckDB database path (default: ym.ddb)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory (default: output)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    logger.info("DuckDB Query Execution Pipeline")
    logger.info("-" * 80)

    with DuckDBQueries(
        database_path=args.database_path,
        output_dir=args.output_dir,
    ) as runner:
        
        res = runner.run_query(
            query="""
            SELECT 
                MAX(ds)
            FROM 
                read_parquet('s3://youthmappers-internal-us-east1/youthmappers_changesets/ds=*/*', hive_partitioning=1)
            """,
            query_name="Fetching latest DS for youthmapper changesets",
        )

        latest_ds = res.fetchall()[0][0]
        logger.info(f"Latest DS for youthmapper changesets: {latest_ds}")

        with open(Path(runner.output_dir, "latest_ds.txt"), 'w') as ds_file:
            ds_file.write(f"{latest_ds}\n")

        runner.run_query(
            query=f"""
                SELECT
                    * EXCLUDE(geometry),
                    h3_latlng_to_cell_string(
                        ST_Y(ST_GeomFromWKB(geometry)), 
                        ST_X(ST_GeomFromWKB(geometry)), 
                        8
                    ) AS h3,
                    ST_GeomFromWKB(geometry) AS geometry
                FROM read_parquet('s3://youthmappers-internal-us-east1/youthmappers_changesets/ds={latest_ds}/*')
            """,
            query_name="Fetching YM changesets and adding h3 indices",
            prefix="CREATE TABLE IF NOT EXISTS changesets AS",
        )

        # Identify the matching ds for YouthMappers table:
        res = runner.run_query(
            query = "SELECT max(changesets.youthmappers_ds) FROM changesets",
            query_name="Getting YouthMappers DS for changesets"
        )
        youthmappers_ds = res.fetchall()[0][0]
        logger.info(f"YouthMappers ds is {youthmappers_ds}")

        runner.run_query(
            query=f"""
                SELECT
                    *,
                    team_id AS chapter_id,
                FROM 
                    read_parquet(
                        's3://youthmappers-internal-us-east1/mappers/ds={youthmappers_ds}/youthmappers.zstd.parquet', hive_partitioning=1
                    )""",
            query_name="Creating YouthMappers table",
            prefix="CREATE TABLE IF NOT EXISTS youthmappers AS",
        )

        # Create Natural Earth Table
        runner.run_query(
            query="SELECT * FROM 'ne_adm0.parquet'",
            query_name="Creating Natural Earth Countries table",
            prefix="CREATE TABLE IF NOT EXISTS natural_earth AS",
        )

        # Bring everything together into one primary table
        runner.run_query(
            query="""
                SELECT
                    changesets.*,
                    youthmappers.* EXCLUDE(uid, team_id, geometry, city, country, username),
                    youthmappers.country AS chapter_country,
                    youthmappers.city AS chapter_city,
                    ST_ASTEXT(youthmappers.geometry) AS chapter_location,
                    natural_earth.* EXCLUDE(geometry, a3),
                FROM changesets JOIN youthmappers ON changesets.uid = youthmappers.uid
                LEFT JOIN natural_earth ON changesets.a3 = natural_earth.a3
            """,
            query_name="Creating Complete YM Changesets Table",
            prefix="CREATE TABLE IF NOT EXISTS ym_changesets AS ",
        )

        # Begin aggregation for anonymization and analysis
        runner.run_query(
            query_path="duckdb.sql:h3_daily_aggregation",
            query_name="H3 Daily Aggregation",
            prefix="CREATE OR REPLACE TABLE changesets_gb_h3_day AS",
        )

        # Required output files for the Activity Dashboard
        # Weekly rollup CSV file
        runner.run_query(
            query_path="duckdb.sql:weekly-rollup",
            query_name="Writing Weekly Chapter Activity Rollup CSV",
            prefix="COPY",
            suffix=f"TO '{runner.output_dir / 'weekly_chapter_activity.csv'}' WITH (FORMAT CSV, HEADER TRUE)"
        )

        # Chapter list JSON file with reference to ds
        res = runner.run_query(
            query="""
                SELECT DISTINCT
                    chapter,
                    chapter_id,
                    chapter_city AS city,
                    chapter_country AS country,
                    university
                FROM ym_changesets""",
            query_name="Fetching distinct chapters for activity.json",
        )
        chapters = res.df()
        chapters = chapters.where(chapters.notna(), None)
        with open(Path(runner.output_dir, "activity.json"), 'w') as out_file:
            out_file.write( 
                json.dumps({
                    'chapters': chapters.sort_values(by='chapter').to_dict(orient='records'),
                    'ds': f"{latest_ds}"
                })
            )

        # Monthly activity
        monthly_activity = runner.run_query(
            query_path="duckdb.sql:monthly-activity",
            query_name="Calculating Monthly Activity"
		).df()
        monthly_activity.to_json(f"{Path(runner.output_dir, 'monthly_activity_all_time.json')}", orient='records')

        # Top edited countries per month
        most_edited_countries = runner.run_query(
            query_path="duckdb.sql:most-edited-countries",
            query_name="Calculating Most Edited Countries"
        ).df()
        top_edited_countries = []
        for month in sorted(most_edited_countries.month.unique()):
            t15 = most_edited_countries[most_edited_countries.month==month].sort_values(by='all_feats', ascending=False).head(15)

            top_edited_countries.append({
                'month': month.isoformat(),
                'top_countries': list(zip(t15.country, t15.all_feats))
            })
        json.dump(top_edited_countries, open(f"{Path(runner.output_dir, 'top_edited_countries.json')}", 'w'))

        # Create the zoom level summaries by week
        for h3_resolution in [6,4]:
            runner.run_query(
                query=f"""
                    SELECT
                        CAST(epoch(date_trunc('week',created_at)) AS int) as timestamp,
                        h3_cell_to_parent(h3, {h3_resolution}) AS h3,
                        arbitrary(chapter_id) as chapter_id,
                        CAST(sum(features.new + features.edited) AS INT) AS all_feats,
                        ST_CENTROID(ST_Union_Agg(geometry)) AS geometry
                    FROM ym_changesets
                    WHERE country IS NOT NULL
                    GROUP BY 1, 2, uid""",
                query_name=f"Creating H3 Resolution {h3_resolution} Weekly Summaries",
                prefix="COPY",
                suffix=f"""TO '{Path(runner.output_dir, f"h3_{h3_resolution}_weekly.geojsonseq")}' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq")"""
            )
            
		# Creating Daily Level Tile Summaries"
        runner.run_query(
            query_path="duckdb.sql:daily-level-tile-summaries",
            query_name="Creating Daily Level Tile Summaries",
            prefix="COPY",
            suffix=f"""TO '{Path(runner.output_dir,"daily_editing_per_user.geojsonseq")}' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq")"""
        )

		# Creating Daily Level bounding boxes
        runner.run_query(
            query_path="duckdb.sql:daily-level-bboxes",
            query_name="Creating Daily Level Bounding Boxes",
            prefix="COPY",
            suffix=f"""TO '{Path(runner.output_dir,"daily_bboxes.geojsonseq")}' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq")"""
		)

        # Create the actual aggregated output for downstream use.
        runner.run_query(
            query_path="duckdb.sql:daily-rollup",
            query_name="Creating Daily Rollup Parquet File (aggregated)",
            prefix="COPY",
            suffix=f"TO '{Path(runner.output_dir,'daily_rollup.parquet')}'",
        )

if __name__ == "__main__":
    main()
