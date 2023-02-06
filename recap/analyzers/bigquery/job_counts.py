from contextlib import contextmanager
from typing import Generator
from urllib.parse import urlparse

from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath


class JobCount(BaseMetadataModel):
    job_count: int
    total_bytes_processed: int
    total_slot_ms: int
    first_job_time: str
    last_job_time: str


class User(BaseMetadataModel):
    __root__: dict[str, JobCount]


class JobCounts(BaseMetadataModel):
    __root__: dict[str, User] = {}


class BigQueryJobCountAnalyzer(AbstractAnalyzer):
    """
    Computes which users have been using a table, and how many jobs (QUERY,
    LOAD, EXTRACT, COPY) they've executed. Also, gets the first and last job
    time for each user.

    This analyzer caches results by region so as not to query the same
    jobs_by_user table over and over again for each dataset/table. Every time a
    dataset from a new region is encountered, the job stats for the entire
    region are calculated using a BigQuery INFORMATION_SCHEMA query. The
    results are cached in-memory for future use with datasets and in the same
    region.

    NOTE: The bytes processed and slot milliseconds statistics include bytes
    and slots for all tables touched in a job. For example, the
    total_bytes_processed a QUERY job that joins two tables will include the
    bytes processed from both input tables.
    """

    def __init__(
        self,
        client: Client,
        days: int = 30,
        region: str = "region-us",
        **_,
    ):
        """
        :param client: BigQuery Client to use when querying.
        :param days: How far back to analyze.
        :param region: The region to query. INFORMATION_SCHEMA.JOBS_BY_USER
            requires this.
        """

        self.client = client
        self.days = days
        self.region = region
        # dict[region, dict[dataset, dict[table, dict[user_email, User]]]]
        self.region_cache: dict[str, dict[str, dict[str, dict[str, User]]]] = {}

    def _load_region_cache(self, region: str):
        if self.region_cache.get(region) is not None:
            # Skip if we've already cache this region.
            return

        region_cache = {}

        # .format variables. BQ doesn't allow @ variables in table names.
        # TODO: This is an injection attack vulnerability.
        project_id = self.client.project

        # param variables
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("project_id", "STRING", self.client.project),
                ScalarQueryParameter("days", "INT64", self.days),
            ],
        )

        sql = f"""
            SELECT
                dataset_id,
                table_id,
                user_email,
                ARRAY_AGG(STRUCT(
                    job_type,
                    job_count,
                    total_bytes_processed,
                    total_slot_ms,
                    first_job_time,
                    last_job_time
                )) job_counts,
            FROM (
                SELECT
                    referenced_table.dataset_id,
                    referenced_table.table_id,
                    user_email,
                    job_type,
                    COUNT(*) as job_count,
                    SUM(total_bytes_processed) as total_bytes_processed,
                    SUM(total_slot_ms) as total_slot_ms,
                    -- For JSON. Uses ISO 8601.
                    CAST(MIN(start_time) AS STRING) as first_job_time,
                    CAST(MAX(start_time) AS STRING) AS last_job_time,
                FROM
                    `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_USER,
                    UNNEST(referenced_tables) referenced_table
                WHERE
                    referenced_table.project_id = @project_id AND
                    -- A NULL value indicates an internal job, such as a script job
                    -- statement evaluation or a materialized view refresh. Ignore.
                    job_type IS NOT NULL AND
                    state = 'DONE' AND
                    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                GROUP BY 1, 2,3 ,4
            ) GROUP BY 1, 2, 3;
        """

        results = self.client.query(sql, job_config=job_config).result()

        for row in results:
            dataset_cache = region_cache.setdefault(row.dataset_id, {})
            table_cache = dataset_cache.setdefault(row.table_id, {})
            user_counts = {}
            for job_counts in row.job_counts:
                user_counts[job_counts["job_type"]] = JobCount.parse_obj(job_counts)
            table_cache[row.user_email] = User.parse_obj(user_counts)

        self.region_cache[region] = region_cache

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> JobCounts | None:
        """
        :param path: The path to analyze for job counts.
        :returns: Query counts for each user, aggregated by job_type.
        """

        dataset_name = path.schema_
        dataset = self.client.get_dataset(dataset_name)
        region_name = dataset.location
        table_name = path.table if isinstance(path, TablePath) else path.view
        if region_name:
            self._load_region_cache(region_name)
            region_counts = self.region_cache.get(region_name, {})
            dataset_counts = region_counts.get(dataset_name, {})
            if job_counts := dataset_counts.get(table_name):
                return JobCounts.parse_obj(job_counts)
        return None


@contextmanager
def create_analyzer(
    url: str,
    **configs,
) -> Generator["BigQueryJobCountAnalyzer", None, None]:
    parsed_url = urlparse(url)
    client = Client(project=parsed_url.hostname)
    yield BigQueryJobCountAnalyzer(client, **configs)
    client.close()
