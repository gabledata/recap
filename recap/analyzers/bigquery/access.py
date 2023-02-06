from contextlib import contextmanager
from typing import Generator
from urllib.parse import urlparse

from google.cloud.bigquery import Client

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import SchemaPath, TablePath, ViewPath


class AccessEntry(BaseMetadataModel):
    role: str
    type: str
    id: str


class Access(BaseMetadataModel):
    __root__: list[AccessEntry] = []


class BigQueryAccessAnalyzer(AbstractAnalyzer):
    """
    Fetches table, view, and dataset access information from BigQuery.

    BigQuery's Python client and access API are a mess. There are two entirely
    different endpoints.

    Tables use `get_iam_policy`, which returns a list of bindings like:

        [{"role": "OWNER", members: ["user:user@email.com"]]

    The dataset API is entirely different. The
    `google.cloud.bigquery.dataset.AccessEntry`s you get are documented
    [here](https://github.com/googleapis/python-bigquery/blob/bd1da9aa0a40b02b7d5409a0b094d8380e255c91/google/cloud/bigquery/dataset.py#L225).

    BigQueryAccessAnalyzer does its best to map these disjoint structures into
    a standard `recap.analyzers.bigquery.access.AccessEntry`.
    """

    def __init__(self, client: Client):
        self.client = client

    def analyze(
        self,
        path: SchemaPath | TablePath | ViewPath,
    ) -> Access | None:
        """
        :param path: Fetch access information for a table, view, or dataset at
            this path.
        :returns: A list of `AccessEntry`s containint a role, id, and the type
            (user, specialGroup, etc) of the id.
        """

        results = []
        match path:
            case TablePath() | ViewPath():
                name = path.table if isinstance(path, TablePath) else path.view
                table_id = f"{self.client.project}.{path.schema_}.{name}"
                policy = self.client.get_iam_policy(table_id)
                for binding in policy.bindings:
                    role = binding["role"]
                    for member in binding["members"]:
                        type_, id = member.split(":")
                        results.append(
                            AccessEntry(
                                role=role,
                                type=type_,
                                id=id,
                            )
                        )
            case SchemaPath():
                dataset = self.client.get_dataset(path.schema_)
                for entry in dataset.access_entries:
                    assert entry.role and entry.entity_type and entry.entity_id
                    id = None
                    match entry.entity_id:
                        case str():
                            id = entry.entity_id
                        case {"projectId": str(), "datasetId": str(), "tableId": str()}:
                            project_id = entry.entity_id["projectId"]
                            dataset_id = entry.entity_id["datasetId"]
                            table_id = entry.entity_id["tableId"]
                            id = f"`{project_id}`.`{dataset_id}`.`{table_id}`"
                        case {
                            "projectId": str(),
                            "datasetId": str(),
                            "routineId": str(),
                        }:
                            project_id = entry.entity_id["projectId"]
                            dataset_id = entry.entity_id["datasetId"]
                            routine_id = entry.entity_id["routineId"]
                            id = f"`{project_id}`.`{dataset_id}`.`{routine_id}`"
                        case {"dataset": _, "target_types": str()}:
                            project_id = entry.entity_id["dataset"]["projectId"]
                            dataset_id = entry.entity_id["dataset"]["datasetId"]
                            target_types = entry.entity_id["target_types"]
                            id = f"`{project_id}`.`{dataset_id}`.`{target_types}`"
                        case _:
                            raise ValueError(f"Couldn't parse AccessEntry={entry}")
                    results.append(
                        AccessEntry(
                            role=entry.role,
                            type=entry.entity_type,
                            id=id,
                        )
                    )
        return Access.parse_obj(results)


@contextmanager
def create_analyzer(
    url: str,
    **_,
) -> Generator["BigQueryAccessAnalyzer", None, None]:
    parsed_url = urlparse(url)
    client = Client(project=parsed_url.hostname)
    yield BigQueryAccessAnalyzer(client)
    client.close()
