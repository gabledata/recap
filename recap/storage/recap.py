import httpx
from .abstract import TableStorage
from contextlib import contextmanager
from os.path import join
from typing import Any, List, Generator


class RecapTableStorage(TableStorage):
    def __init__(
        self,
        infra: str,
        instance: str,
        client: httpx.Client,
    ):
        super().__init__(infra, instance)
        self.client = client

    def put_instance(self):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
        ))

    def put_schema(self, schema: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        ))

    def put_table(self, schema: str, table: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables', table,
        ))

    def put_view(self, schema: str, view: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views', view,
        ))

    def put_metadata(
        self,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.put(path, json=metadata)

    def remove_instance(self):
        self.client.delete(join(
            'databases', self.infra,
            'instances', self.instance,
        ))

    def remove_schema(self, schema: str):
        self.client.delete(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        ))

    def remove_table(self, schema: str, table: str):
        self.client.delete(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables', table,
        ))

    def remove_view(self, schema: str, view: str):
        self.client.delete(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views', view,
        ))

    def remove_metadata(
        self,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.delete(path)

    def list_schemas(self) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas'
        )).json()

    def list_tables(self, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables'
        )).json()

    def list_views(self, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views'
        )).json()

    def list_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata')
        return self.client.get(path).json()

    def get_metadata(
        self,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.get(path).json()


@contextmanager
def open(**config) -> Generator[RecapTableStorage, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        # TODO Remove inra/instance once TableStorage changes to Storage
        yield RecapTableStorage(
            # TODO Remove hardcoding
            'postgresql',
            'sticker_space_dev',
            client,
        )
