import httpx
from .abstract import AbstractStorage
from contextlib import contextmanager
from os.path import join
from typing import Any, List, Generator


class RecapStorage(AbstractStorage):
    def __init__(
        self,
        client: httpx.Client,
    ):
        self.client = client

    def put_instance(self, infra: str, instance: str):
        self.client.put(join(
            'databases', infra,
            'instances', instance,
        ))

    def put_schema(self, infra: str, instance: str, schema: str):
        self.client.put(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
        ))

    def put_table(self, infra: str, instance: str, schema: str, table: str):
        self.client.put(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'tables', table,
        ))

    def put_view(self, infra: str, instance: str, schema: str, view: str):
        self.client.put(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'views', view,
        ))

    def put_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        path = join('databases', infra, 'instances', instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.put(path, json=metadata)

    def remove_instance(self, infra: str, instance: str):
        self.client.delete(join(
            'databases', infra,
            'instances', instance,
        ))

    def remove_schema(self, infra: str, instance: str, schema: str):
        self.client.delete(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
        ))

    def remove_table(self, infra: str, instance: str, schema: str, table: str):
        self.client.delete(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'tables', table,
        ))

    def remove_view(self, infra: str, instance: str, schema: str, view: str):
        self.client.delete(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'views', view,
        ))

    def remove_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        path = join('databases', infra, 'instances', instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.delete(path)

    def list_infra(self) -> List[str]:
        return self.client.get('databases').json()

    def list_instances(self, infra: str) -> List[str]:
        return self.client.get(join(
            'databases', infra,
            'instances'
        )).json()

    def list_schemas(self, infra: str, instance: str) -> List[str]:
        return self.client.get(join(
            'databases', infra,
            'instances', instance,
            'schemas'
        )).json()

    def list_tables(self, infra: str, instance: str, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'tables'
        )).json()

    def list_views(self, infra: str, instance: str, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'views'
        )).json()

    def list_metadata(
        self,
        infra: str,
        instance: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', infra, 'instances', instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata')
        return self.client.get(path).json()

    def get_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', infra, 'instances', instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        return self.client.get(path).json()


@contextmanager
def open(**config) -> Generator[RecapStorage, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        yield RecapStorage(client)
