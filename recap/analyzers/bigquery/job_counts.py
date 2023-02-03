from contextlib import contextmanager
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath
from typing import Generator
from urllib.parse import urlparse


class QueryCount(BaseMetadataModel):
    query_count: int
    total_bytes_processed: int
    total_slot_ms: int
    first_job_time: str
    last_job_time: str


class User(BaseMetadataModel):
    __root__: dict[str, QueryCount]


class QueryCounts(BaseMetadataModel):
    __root__: dict[str, User] = {}


class BigQueryJobCountAnalyzer(AbstractAnalyzer):
    """
    Computes which users have been using a table, and how many jobs (QUERY,
    LOAD, EXTRACT, COPY) they've executed. Also provides the first and last job
    time for each user.

    NOTE: The bytes processed and slot milliseconds statistics include bytes
    and slots for all tables touched in a job. For example, the
    total_bytes_processed a QUERY job that joins two tables will include the
    bytes processed from both input tables.
    """

    def __init__(
        self,
        client: Client,
        days: int = 30,
        limit: int = 1000,
        region: str = 'region-us',
        **_,
    ):
        """
        :param client: BigQuery Client to use when querying.
        :param days: How far back to analyze.
        :param limit: Maximum number of rows to return. If limit is set, some
            users might contain only some job types since other job types might
            fall below the limit.
        :param region: The region to query. INFORMATION_SCHEMA.JOBS_BY_USER
            requires this.
        """

        self.client = client
        self.days = days
        self.limit = limit
        self.region = region

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> QueryCounts | None:
        """
        :param path: The path to analyze for job counts.
        :returns: Query counts for each user, aggregated by job_type.
        """

        users = {}
        name = path.table if isinstance(path, TablePath) else path.view

        # .format variables
        project_id = self.client.project
        region = self.region

        # param variables
        job_config = QueryJobConfig(
            query_parameters = [
                ScalarQueryParameter("project_id", "STRING", self.client.project),
                ScalarQueryParameter("dataset_id", "STRING", path.schema_),
                ScalarQueryParameter("table_id", "STRING", name),
                ScalarQueryParameter("days", "INT64", self.days),
                ScalarQueryParameter("limit_", "INT64", self.limit),
            ],
        )

        sql = f"""
            SELECT
                user_email,
                job_type,
                COUNT(*) as query_count,
                SUM(total_bytes_processed) as total_bytes_processed,
                SUM(total_slot_ms) as total_slot_ms,
                -- For JSON. Uses ISO 8601.
                CAST(MIN(start_time) AS STRING) as first_job_time,
                CAST(MAX(start_time) AS STRING) AS last_job_time,
            FROM
                `{project_id}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_USER,
                UNNEST(referenced_tables) referenced_table
            WHERE
                referenced_table.project_id = @project_id AND
                referenced_table.dataset_id = @dataset_id AND
                referenced_table.table_id = @table_id AND
                -- A NULL value indicates an internal job, such as a script job
                -- statement evaluation or a materialized view refresh. Ignore.
                job_type IS NOT NULL AND
                state = 'DONE' AND
                creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT @limit_
        """
        results = self.client.query(sql, job_config=job_config).result()

        for row in results:
            user = users.get(row.user_email, {})
            user[row.job_type] = dict(row)
            users[row.user_email] = user

        return QueryCounts.parse_obj(users)

@contextmanager
def create_analyzer(
    url: str,
    **configs,
) -> Generator['BigQueryJobCountAnalyzer', None, None]:
    parsed_url = urlparse(url)
    client = Client(project=parsed_url.hostname)
    yield BigQueryJobCountAnalyzer(client, **configs)
    client.close()
