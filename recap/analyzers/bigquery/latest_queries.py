from contextlib import contextmanager
from google.cloud.bigquery import Client, QueryJobConfig, ScalarQueryParameter
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath
from typing import Generator
from urllib.parse import urlparse


class QueryJobError(BaseMetadataModel):
    reason: str | None
    message: str | None


class QueryJob(BaseMetadataModel):
    job_id: str
    query: str
    statement_type: str
    user_email: str
    start_time: str
    end_time: str
    priority: str
    total_bytes_processed: int | None
    total_slot_ms: int | None
    error: QueryJobError | None


class QueryJobs(BaseMetadataModel):
    __root__: list[QueryJob] = []


class BigQueryLatestQueriesAnalyzer(AbstractAnalyzer):
    """
    Retrieves the most recent queries on a table or view.

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
        :param limit: Maximum number of rows to return.
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
    ) -> QueryJobs | None:
        """
        :param path: The path to gather queries for.
        :returns: Most recent queries. Includes the query, the user, and
            various statistics about the query.
        """

        jobs = []
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
                job_id,
                query,
                state,
                user_email,
                -- For JSON. Uses ISO 8601.
                CAST(start_time AS STRING) start_time,
                CAST(end_time AS STRING) end_time,
                priority,
                total_bytes_processed,
                total_slot_ms,
                error_result,
                statement_type,
            FROM
                `{project_id}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_USER,
                UNNEST(referenced_tables) referenced_table
            WHERE
                referenced_table.project_id = @project_id AND
                referenced_table.dataset_id = @dataset_id AND
                referenced_table.table_id = @table_id AND
                state = 'DONE' AND
                job_type = 'QUERY' AND
                creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
            ORDER BY creation_time DESC
            LIMIT @limit_
        """
        results = self.client.query(sql, job_config=job_config).result()

        for row in results:
            row_dict = dict(row)
            if error_result := row_dict.get('error_result'):
                row_dict['error'] = QueryJobError.parse_obj(dict(error_result)) # pyright: ignore [reportGeneralTypeIssues]
            jobs.append(QueryJob.parse_obj(dict(row_dict)))

        return QueryJobs.parse_obj(jobs)

@contextmanager
def create_analyzer(
    url: str,
    **configs,
) -> Generator['BigQueryLatestQueriesAnalyzer', None, None]:
    parsed_url = urlparse(url)
    client = Client(project=parsed_url.hostname)
    yield BigQueryLatestQueriesAnalyzer(client, **configs)
    client.close()
