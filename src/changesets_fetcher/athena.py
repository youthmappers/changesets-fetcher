"""AWS Athena query execution with role assumption and waiters."""

import argparse
import logging
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

from .utils import load_query_from_path


class AthenaQueryRunner:
    """Execute Athena queries with proper waiting and error handling."""

    running_queries: dict[str, str] = {}

    def __init__(
        self,
        role_arn: str | None = None,
        region: str | None = None,
        output_location: str | None = None,
        workgroup: str | None = None,
        database: str | None = None,
    ):
        """Initialize the Athena query runner.

        Args:
            role_arn: AWS IAM role ARN (defaults to YOUTHMAPPERS_AWS_ROLE env var)
            region: AWS region (defaults to YOUTHMAPPERS_AWS_REGION env var or us-east-1)
            output_location: S3 output location (defaults to YOUTHMAPPERS_ATHENA_OUTPUT_LOCATION)
            workgroup: Athena workgroup (defaults to YOUTHMAPPERS_ATHENA_WORKGROUP)
            database: Athena database name (defaults to YOUTHMAPPERS_ATHENA_DATABASE)
        """
        import os

        # Use environment variables with sensible defaults
        self.region = region or os.getenv("YOUTHMAPPERS_AWS_REGION", "us-east-1")
        self.output_location = output_location or os.getenv(
            "YOUTHMAPPERS_ATHENA_OUTPUT_LOCATION",
            "s3://youthmappers-internal-us-east1/athena-results/",
        )
        self.workgroup = workgroup or os.getenv("YOUTHMAPPERS_ATHENA_WORKGROUP", "youthmappers")
        self.database = database or os.getenv("YOUTHMAPPERS_ATHENA_DATABASE", "youthmappers")
        role_arn = role_arn or os.getenv("YOUTHMAPPERS_AWS_ROLE")

        logger.info("Athena configuration:")
        logger.info(f"  Region: {self.region}")
        logger.info(f"  Database: {self.database}")
        logger.info(f"  Workgroup: {self.workgroup}")
        logger.info(f"  Output location: {self.output_location}")
        if role_arn:
            logger.info(f"  Role ARN: {role_arn}")

        self.session = self._create_session(role_arn)
        self.athena_client = self.session.client("athena", region_name=self.region)
        self.s3_client = self.session.client("s3", region_name=self.region)
        self.glue_client = self.session.client("glue", region_name=self.region)

    def _create_session(self, role_arn: str | None) -> boto3.Session:
        """Create a boto3 session, assuming a role if specified.

        Args:
            role_arn: AWS IAM role ARN to assume

        Returns:
            Configured boto3 Session
        """
        if role_arn:
            logger.info(f"Assuming role: {role_arn}")
            sts_client = boto3.client("sts")
            try:
                assumed_role = sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="changesets-fetcher-session",
                    DurationSeconds=3600,
                )
                credentials = assumed_role["Credentials"]
                return boto3.Session(
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                )
            except ClientError as e:
                logger.error(f"Failed to assume role: {e}")
                raise
        else:
            logger.info("Using default AWS credentials")
            return boto3.Session()

    def run_query(
        self,
        query: str | None = None,
        query_path: str | None = None,
        query_name: str = "query",
        prefix: str | None = None,
        suffix: str | None = None,
    ) -> str:
        """Run an Athena query, optionally waiting for completion.

        Note: Database must be explicitly set in the query (e.g., USE database; or database.table)

        Args:
            query: SQL query to execute (string)
            query_path: "filename:query-name" reference to a SQL file block
            query_name: Name for logging purposes
            prefix: Optional SQL to prepend (query will be wrapped in parentheses)
            suffix: Optional SQL to append (query will be wrapped in parentheses)

        Returns:
            Query execution ID

        Raises:
            ValueError: If neither query nor query_path is provided
            FileNotFoundError: If the SQL file cannot be found
            KeyError: If the query block is not found in the SQL file
            Exception: If query submission fails
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

        logger.debug(f"Query: {resolved_query[:200]}...")

        try:
            response = self.athena_client.start_query_execution(
                QueryString=resolved_query,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.output_location},
                WorkGroup=self.workgroup,
            )
            query_execution_id = response["QueryExecutionId"]
            logger.info(f"✓ {query_name} submitted - {query_execution_id}")

            self.running_queries[query_name] = query_execution_id

            return query_execution_id

        except ClientError as e:
            logger.error(f"Failed to start {query_name}: {e}")
            raise

    def wait_for_queries(self) -> None:
        """Wait for all currently running Athena queries to complete.

        Raises:
            Exception: If any query fails
        """
        if not self.running_queries:
            logger.warning("No queries to wait for")
            return

        total_queries = len(self.running_queries)
        logger.info(f"Waiting for {total_queries} queries to complete...")
        logger.info("-" * 80)

        start_time = time.time()
        failed_queries: list[str] = []
        completed_stats: dict[str, dict[str, float]] = {}

        initial_interval = 1.0
        max_interval = 10.0
        backoff = 1.5
        interval = initial_interval

        while self.running_queries:
            status_counts: dict[str, int] = {}

            for query_name, query_execution_id in list(self.running_queries.items()):
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                status_info = response["QueryExecution"]["Status"]
                state = status_info["State"]
                status_counts[state] = status_counts.get(state, 0) + 1

                if state == "SUCCEEDED":
                    stats = response["QueryExecution"].get("Statistics", {})
                    completed_stats[query_name] = {
                        "execution_time_ms": float(stats.get("EngineExecutionTimeInMillis", 0)),
                        "data_scanned_bytes": float(stats.get("DataScannedInBytes", 0)),
                    }
                    self.running_queries.pop(query_name, None)
                elif state in {"FAILED", "CANCELLED"}:
                    reason = status_info.get("StateChangeReason", "Unknown")
                    logger.error(
                        f"{query_name} {state}: {reason} (Execution ID: {query_execution_id})"
                    )
                    failed_queries.append(query_name)
                    self.running_queries.pop(query_name, None)

            if self.running_queries:
                status_summary = ", ".join(
                    f"{state}={count}" for state, count in sorted(status_counts.items())
                )
                logger.info(f"Query status: {status_summary}")
                time.sleep(interval)
                interval = min(max_interval, interval * backoff)

        elapsed = time.time() - start_time

        logger.info("-" * 80)
        logger.info(f"Query execution summary ({elapsed:.2f}s total):")
        logger.info(f"  Total: {total_queries}")
        logger.info(f"  Succeeded: {total_queries - len(failed_queries)}")
        logger.info(f"  Failed: {len(failed_queries)}")

        for query_name, stats in completed_stats.items():
            execution_time = stats["execution_time_ms"] / 1000
            data_scanned = stats["data_scanned_bytes"] / (1024**3)
            logger.info(
                f"  - {query_name}: execution {execution_time:.2f}s, data scanned {data_scanned:.2f} GB"
            )

        if failed_queries:
            logger.error(f"Failed queries: {', '.join(failed_queries)}")
            raise Exception(f"{len(failed_queries)} queries failed")

    def add_partitions_with_glue(
        self,
        table_name: str,
        s3_prefix: str,
        partition_key: str = "ds",
        database: str | None = None,
    ) -> int:
        """Add partitions using AWS Glue batch_create_partition.

        Args:
            table_name: Target table name (optionally schema-qualified)
            s3_prefix: S3 URI prefix like s3://bucket/path/
            partition_key: Partition key name (default: ds)
            database: Glue database override (defaults to runner database)

        Returns:
            Number of partitions successfully created
        """

        partitions, bucket = self._list_partitions_from_s3(s3_prefix, partition_key)
        if not partitions:
            logger.warning(f"No partitions found for {table_name} under {s3_prefix}")
            return 0

        table = self.glue_client.get_table(
            DatabaseName=self.database,
            Name=table_name,
        )["Table"]

        base_sd = table["StorageDescriptor"].copy()

        partition_inputs = []
        for value, prefix in partitions:
            location = f"s3://{bucket}/{prefix}"
            sd = base_sd.copy()
            sd["Location"] = location
            partition_inputs.append(
                {
                    "Values": [value],
                    "StorageDescriptor": sd,
                }
            )

        created = 0
        for i in range(0, len(partition_inputs), 100):
            batch = partition_inputs[i : i + 100]
            response = self.glue_client.batch_create_partition(
                DatabaseName=self.database,
                TableName=table_name,
                PartitionInputList=batch,
            )
            errors = response.get("Errors", [])
            created += len(batch) - len(errors)

            for err in errors:
                error_detail = err.get("ErrorDetail", {})
                message = error_detail.get("ErrorMessage", "Unknown")
                if "AlreadyExistsException" in message:
                    continue
                logger.error(
                    "Glue partition error for %s: %s",
                    err.get("PartitionValues"),
                    message,
                )

        logger.info("✓ Glue partitions created: %s", created)
        return created

    def _list_partitions_from_s3(
        self,
        s3_prefix: str,
        partition_key: str,
    ) -> tuple[list[tuple[str, str]], str]:
        """List S3 partition prefixes and return (value, prefix) tuples and bucket."""
        if not s3_prefix.startswith("s3://"):
            raise ValueError("s3_prefix must be a full S3 URI (s3://bucket/path/)")

        bucket_and_prefix = s3_prefix.replace("s3://", "", 1)
        if "/" not in bucket_and_prefix:
            raise ValueError("s3_prefix must include a path after the bucket name")

        bucket, prefix = bucket_and_prefix.split("/", 1)
        prefix = prefix.rstrip("/") + "/"

        paginator = self.s3_client.get_paginator("list_objects_v2")
        common_prefixes: list[str] = []

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                common_prefixes.append(item["Prefix"])

        partitions: list[tuple[str, str]] = []
        for p in common_prefixes:
            remainder = p[len(prefix) :].strip("/")
            if remainder.startswith(f"{partition_key}="):
                value = remainder.split("=", 1)[1]
                partitions.append((value, p))

        return partitions, bucket


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
    """Main entry point for Athena pipeline."""
    parser = argparse.ArgumentParser(
        description="YouthMappers Athena data pipeline - explicit query execution"
    )
    parser.add_argument(
        "--role-arn",
        default=os.environ.get("YOUTHMAPPERS_AWS_ROLE"),
        help="AWS IAM role ARN to assume (default: YOUTHMAPPERS_AWS_ROLE env var)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("YOUTHMAPPERS_AWS_REGION"),
        help="AWS region (default: YOUTHMAPPERS_AWS_REGION env var)",
    )
    parser.add_argument(
        "--output-location",
        default=os.environ.get("YOUTHMAPPERS_ATHENA_OUTPUT_LOCATION"),
        help="Athena output S3 location (default: YOUTHMAPPERS_ATHENA_OUTPUT_LOCATION env var)",
    )
    parser.add_argument(
        "--workgroup",
        default=os.environ.get("YOUTHMAPPERS_ATHENA_WORKGROUP"),
        help="Athena workgroup (default: YOUTHMAPPERS_ATHENA_WORKGROUP env var)",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("YOUTHMAPPERS_ATHENA_DATABASE"),
        help="Athena database (default: YOUTHMAPPERS_ATHENA_DATABASE env var)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    logger.info("YouthMappers Athena Data Pipeline\n")

    logger.info("\nInitializing Athena query runner...")

    runner = AthenaQueryRunner(
        role_arn=args.role_arn,
        region=args.region,
        output_location=args.output_location,
        workgroup=args.workgroup,
        database=args.database,
    )

    logger.info("-" * 80)
    logger.info("Beginning Athena Query Execution Pipeline")
    logger.info("-" * 80)

    # Step 1: Create database if not exists yet
    runner.run_query(
        query=f"CREATE DATABASE IF NOT EXISTS {runner.database}",
        query_name=f"Create database: {runner.database}",
    )

    runner.wait_for_queries()

    # Step 2: Create external tables
    runner.run_query(
        query_path="tables.sql:osm-pds-changesets",
        query_name="OSM Public Dataset: Changesets",
    )

    runner.run_query(
        query_path="tables.sql:osm-pds-planet-history",
        query_name="OSM Public Dataset: Planet History",
    )

    runner.run_query(
        query_path="tables.sql:osm-pds-planet",
        query_name="OSM Public Dataset: Planet",
    )

    runner.run_query(
        query_path="tables.sql:natural-earth-boundaries",
        query_name="Natural Earth Admin 0 Boundaries",
    )

    runner.run_query(
        query_path="tables.sql:youthmappers-from-parquet",
        query_name="YouthMappers GeoParquet from OSM Teams",
    )

    # Block for above queries to complete
    runner.wait_for_queries()

    runner.add_partitions_with_glue(
        table_name="youthmappers", s3_prefix="s3://youthmappers-internal-us-east1/mappers/"
    )

    # Step 3: Run the primary YouthMappers unload query to create parquet files
    runner.run_query(
        query_path="youthmappers_query.sql",
        query_name="Create YouthMappers Parquet Files",
        prefix="UNLOAD",
        suffix=""" 
          TO 's3://youthmappers-internal-us-east1/youthmappers_changesets/'
          WITH (
                format = 'PARQUET',
                compression = 'ZSTD',
                partitioned_by = ARRAY['ds']
            )""",
    )

    runner.wait_for_queries()


if __name__ == "__main__":
    main()
