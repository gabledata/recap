from __future__ import annotations

from google.cloud.bigquery import Client
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from recap.registry import registry
from recap.schema.converters.bigquery import to_recap_schema
from recap.schema.model import Schema
from recap.storage.abstract import Direction


@registry.relationship("bigquery://{project}", "contains", include_engine=True)
@registry.relationship(
    "bigquery://{project}/{dataset}", "contains", include_engine=True
)
def ls(
    engine: Engine,
    project: str,
    dataset: str | None = None,
    **_,
) -> list[str]:
    """
    List all URLs contained by a project and (optionally) a dataset. If project
    is set, returned URLs will be datasets:
        `bigquery://some-project-1234/some_dataset`
    If dataset is set, returned URLs will be tables:
        `bigquery://some-project-1234/some_dataset/some_table`

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param project: A google cloud project ID (e.g. `some-project-1234`)
    :param dataset: A dataset name.
    :returns: A list of dataset or table URIs.
    """

    if dataset:
        tables = inspect(engine).get_table_names(dataset)
        views = inspect(engine).get_view_names(dataset)
        return [
            # Remove schema prefix if it's there
            f"bigquery://{project}/{dataset}/{table.split('.')[-1]}"
            for table in tables + views
        ]
    else:
        return [
            f"bigquery://{project}/{dataset}"
            for dataset in inspect(engine).get_schema_names()
        ]


@registry.relationship(
    "bigquery://{project}/{dataset}/{table}", "reads", Direction.TO, include_engine=True
)
def readers(
    engine: Engine,
    project: str,
    dataset: str,
    table: str,
    **client_args,
) -> list[str]:
    """
    Returns all accounts and jobs that read from a BigQuery table. Return URLs
    will be of the form:

        bigquery://some-project?account=user:some@email.com
        bigquery://some-project?job=bquxjob123456

    A reader account signals read-access. A reader job signals that the job
    read from the dataset.

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param project: A google cloud project ID (e.g. `some-project-1234`)
    :param dataset: A dataset name.
    :param table: A table name.
    :param args: Extra arguments passed to a `google.cloud.bigquery.Client`.
    :returns: A list of dataset or table URIs.
    """

    return readers_accounts(project, dataset, table, **client_args) + readers_jobs(
        engine, project, dataset, table
    )


def readers_accounts(
    project: str,
    dataset: str,
    table: str,
    **client_args,
) -> list[str]:
    client = Client(project, **client_args)
    if policy := client.get_iam_policy(f"{dataset}.{table}"):
        for binding in policy.bindings:
            if is_reader(binding.get("role", "")):
                return [
                    f"bigquery://{project}?account={member}"
                    for member in binding.get("members", [])
                ]
    return []


def readers_jobs(
    engine: Engine,
    project: str,
    dataset: str,
    table: str,
) -> list[str]:
    # TODO Some day, maybe data plex's lineage API will work for this.
    from sqlalchemy.sql import text

    # TODO Should get region for dataset, and use that in query.
    with engine.connect() as connection:
        results = connection.execute(
            text(
                """
            SELECT
                job_id,
                query,
                destination_table,
            FROM
                `region-us`.INFORMATION_SCHEMA.JOBS,
                UNNEST(referenced_tables) AS referenced_table
            WHERE
                referenced_table.project_id = :project_id
                AND referenced_table.dataset_id = :dataset_id
                AND referenced_table.table_id = :table_id
                AND state = 'DONE'
            """
            ),
            project_id=project,
            dataset_id=dataset,
            table_id=table,
        )

        return [f"bigquery://{project}?job={row[0]}" for row in results]


@registry.metadata("bigquery://{project}/{dataset}/{table}")
def schema(
    project: str,
    dataset: str,
    table: str,
    **client_args,
) -> Schema:
    """
    Fetch a schema from a BigQuery table.

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param project: A google cloud project ID (e.g. `some-project-1234`)
    :param dataset: A dataset name.
    :param table: A table name.
    :returns: A Recap schema.
    """

    client = Client(project, **client_args)
    table_props = client.get_table(f"{project}.{dataset}.{table}")
    return to_recap_schema(table_props.schema)


@registry.relationship(
    "bigquery://{project}/{dataset}/{table}",
    "writes",
    Direction.TO,
    include_engine=True,
)
def writers(
    engine: Engine,
    project: str,
    dataset: str,
    table: str,
    **client_args,
) -> list[str]:
    """
    Returns all accounts and jobs that write to a BigQuery table. Return URLs
    will be of the form:

        bigquery://some-project?account=user:some@email.com
        bigquery://some-project?job=bquxjob123456

    A writer account signals write-access. A writer job signals that the job
    wrote to the dataset.

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param project: A google cloud project ID (e.g. `some-project-1234`)
    :param dataset: A dataset name.
    :param table: A table name.
    :param client_args: Extra arguments passed to a
        `google.cloud.bigquery.Client`.
    :returns: A list of dataset or table URIs.
    """

    return writers_accounts(project, dataset, table, **client_args) + writers_jobs(
        engine, project, dataset, table
    )


def writers_accounts(
    project: str,
    dataset: str,
    table: str,
    **client_args,
) -> list[str]:
    client = Client(project, **client_args)
    if policy := client.get_iam_policy(f"{dataset}.{table}"):
        for binding in policy.bindings:
            if is_writer(binding.get("role", "")):
                return [
                    f"bigquery://{project}?account={member}"
                    for member in binding.get("members", [])
                ]
    return []


def writers_jobs(
    engine: Engine,
    project: str,
    dataset: str,
    table: str,
) -> list[str]:
    # TODO Some day, maybe dataplex's lineage API will work for this.
    from sqlalchemy.sql import text

    # TODO Should get region for dataset, and use that in query.
    with engine.connect() as connection:
        results = connection.execute(
            text(
                """
            SELECT
                job_id,
            FROM
                `region-us`.INFORMATION_SCHEMA.JOBS,
                UNNEST(referenced_tables)
            WHERE
                destination_table.project_id = :project_id
                AND destination_table.dataset_id = :dataset_id
                AND destination_table.table_id = :table_id
                AND state = 'DONE'
            """
            ),
            project_id=project,
            dataset_id=dataset,
            table_id=table,
        )

        return [f"bigquery://{project}?job={row[0]}" for row in results]


"""
NOTE: This is certainly not accurate. The correct way to do this is to query
the permissions for a given role, and see if they have the
`bigquery.tables.read` permission, and so on. Unfortunately, Google's IAM API
is a bit of a mess right now, and this query doesn't appear possible in their
V2 client.

For now, I'm just supporting the pre-defined roles.
"""

READERS = set(
    [
        "roles/owner",
        "roles/editor",
        "roles/viewer",
        "roles/bigquery.admin",
        "roles/bigquery.dataEditor",
        "roles/bigquery.dataOwner",
        "roles/bigquery.dataViewer",
        "roles/bigquery.filteredDataViewer",
        "roles/bigquerydatapolicy.maskedReader",
    ]
)

WRITERS = set(
    [
        "roles/owner",
        "roles/editor",
        "roles/bigquery.admin",
        "roles/bigquery.dataEditor",
        "roles/bigquery.dataOwner",
    ]
)


def is_reader(role: str) -> bool:
    return role in READERS


def is_writer(role: str) -> bool:
    return role in WRITERS
