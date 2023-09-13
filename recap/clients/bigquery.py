from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from google.cloud import bigquery

from recap.converters.bigquery import BigQueryConverter
from recap.types import StructType


class BigQueryClient:
    def __init__(self, client: bigquery.Client):
        self.client = client

    @staticmethod
    @contextmanager
    def create(**_) -> Generator[BigQueryClient, None, None]:
        with bigquery.Client() as client:
            yield BigQueryClient(client)

    @staticmethod
    def parse(method: str, paths: list[str], **_) -> tuple[str, list[Any]]:
        project, dataset, table = (paths + [None, None, None])[:3]

        match method:
            case "ls" | "schema":
                return ("bigquery://", [project, dataset, table])
            case _:
                raise ValueError("Invalid method")

    def ls(self, project: str | None = None, dataset: str | None = None) -> list[str]:
        match (project, dataset):
            case (None, None):
                return self.ls_projects()
            case (str(project), None):
                return self.ls_datasets(project)
            case (str(project), str(dataset)):
                return self.ls_tables(project, dataset)
            case _:
                raise ValueError("Invalid arguments")

    def ls_projects(self) -> list[str]:
        return [project.project_id for project in self.client.list_projects()]

    def ls_datasets(self, project: str) -> list[str]:
        return [dataset.dataset_id for dataset in self.client.list_datasets(project)]

    def ls_tables(self, project: str, dataset: str) -> list[str]:
        dataset_ref = self.client.dataset(dataset, project)
        return [table.table_id for table in self.client.list_tables(dataset_ref)]

    def schema(self, project: str, dataset: str, table: str, **_) -> StructType:
        table_ref = self.client.dataset(dataset, project).table(table)
        table_obj = self.client.get_table(table_ref)
        return BigQueryConverter().to_recap(table_obj.schema)
