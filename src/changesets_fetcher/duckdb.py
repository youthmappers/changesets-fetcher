"""DuckDB query processor for local data analysis."""

import argparse
import logging
import os
import sys
from pathlib import Path

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

        logger.info(f"Executing {query_name}")
        stmt_preview = resolved_query[:100].replace("\n", " ")
        logger.debug(f"Query preview: {stmt_preview}...")

        result = self.conn.execute(resolved_query)

        return result.fetchall()

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
    parser.add_argument(
        "--no-fetch-from-s3",
        action="store_true",
        help="Skip the initial S3 fetch queries (assumes tables already exist)",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    logger.info("DuckDB Query Execution Pipeline")
    logger.info("-" * 80)

    with DuckDBQueries(
        database_path=args.database_path,
        output_dir=args.output_dir,
    ) as runner:
        if args.no_fetch_from_s3:
            logger.info("Skipping S3 fetch queries (--no-fetch-from-s3 enabled)")
        else:
            res = runner.run_query(
                query="""
                SELECT 
                    MAX(ds)
                FROM 
                    read_parquet('s3://youthmappers-internal-us-east1/youthmappers_changesets/ds=*/*', hive_partitioning=1)
                """,
                query_name="Fetch latest DS for youthmapper changesets",
            )

            latest_ds = res[0][0]

            logger.info(f"Latest DS for youthmapper changesets: {latest_ds}")

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
                query_name="Fetch YM changesets and add h3 indices",
                prefix="CREATE OR REPLACE TABLE changesets AS",
            )

            runner.run_query(
                query="""
                    SELECT
                        *
                    FROM 
                        read_parquet(
                            's3://youthmappers-internal-us-east1/mappers/ds=*/youthmappers.zstd.parquet', hive_partitioning=1
                        )""",
                query_name="Fetch YouthMappers table",
                prefix="CREATE OR REPLACE TABLE youthmappers AS",
            )

        runner.run_query(
            query="SELECT * FROM 'ne_adm0.parquet'",
            query_name="Create Natural Earth Countries table",
            prefix="CREATE OR REPLACE TABLE natural_earth AS",
        )

        runner.run_query(
            query="""
                SELECT youthmappers.* 
                FROM youthmappers 
                WHERE youthmappers.ds = (SELECT max(changesets.youthmappers_ds) FROM changesets)
            """,
            query_name="Create latest_youthmappers table",
            prefix="CREATE OR REPLACE TABLE latest_youthmappers AS",
        )

        # Bring everything together into one primary table
        runner.run_query(
            query="""
                SELECT
                    changesets.*,
                    latest_youthmappers.* EXCLUDE(uid, geometry, city, country, username),
                    latest_youthmappers.country AS chapter_country,
                    latest_youthmappers.city AS chapter_city,
                    ST_ASTEXT(latest_youthmappers.geometry) AS chapter_location,
                    natural_earth.* EXCLUDE(geometry, a3),
                FROM changesets JOIN latest_youthmappers ON changesets.uid = latest_youthmappers.uid
                LEFT JOIN natural_earth ON changesets.a3 = natural_earth.a3
            """,
            query_name="Create Complete YM Changesets Table",
            prefix="CREATE OR REPLACE TABLE ym_changesets AS ",
        )

        # Begin aggregation for anonymization and analysis
        runner.run_query(
            query_path="duckdb.sql",
            query_name="H3 Daily Aggregation",
            prefix="CREATE OR REPLACE TABLE changesets_gb_h3_day AS",
        )

        # Create the actual aggregated output for downstream use.
        runner.run_query(
            query="""
            SELECT 
                -- Changesets aggregated by h3 and day, exlude UID
                changesets_gb_h3_day.* EXCLUDE(uid, geometry),
                
                -- YouthMappers Chapter Information:
                latest_youthmappers.team_id AS chapter_id,
                latest_youthmappers.chapter,
                latest_youthmappers.country AS chapter_country,
                latest_youthmappers.city AS chapter_city,
                ST_ASTEXT(latest_youthmappers.geometry) AS chapter_location,
                
                -- Country where the editing took place:
                natural_earth.a3 AS a3,
                natural_earth.country AS country,
                natural_earth.name AS country_name,
                natural_earth.region AS region,
                -- And include the geometry of the centroid for the heatmap
                ST_CENTROID(changesets_gb_h3_day.geometry) AS geometry
            FROM changesets_gb_h3_day JOIN latest_youthmappers ON changesets_gb_h3_day.uid = latest_youthmappers.uid
            LEFT JOIN natural_earth ON 
                ST_Contains(
                    natural_earth.geometry, 
                    ST_CENTROID(changesets_gb_h3_day.geometry)
                )
            """,
            query_name="YM Changesets Aggregated",
            prefix="COPY",
            suffix="TO 'daily_rollup.parquet'",
        )


if __name__ == "__main__":
    main()
